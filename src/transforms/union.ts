import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats, _IIndex, _IType, setId, getId, _IAggregation, Row } from '../interfaces-private';
import { DataSourceBase } from './transform-base';
import { ColumnNotFound, nil, NotSupported, QueryError } from '../interfaces';
import { columnEvaluator } from './selection';
import { reconciliateTypes } from '../datatypes/datatypes';
import { ExprRef } from 'pgsql-ast-parser';
import { colByName } from '../utils';

// https://www.postgresql.org/docs/current/typeconv-union-case.html
function reconcileSetOpCols(left: _ISelection, right: _ISelection, verb: string): UCol[] {
    if (left.columns.length !== right.columns.length) {
        throw new QueryError(`each ${verb} query must have the same number of columns`);
    }
    const cols: UCol[] = Array(left.columns.length);
    for (let i = 0; i < left.columns.length; i++) {
        const l = left.columns[i];
        const r = right.columns[i];

        const type = reconciliateTypes([l, r], true, true);
        if (!type) {
            throw new QueryError(`${verb} types ${l.type.name} (${l.id ?? '<unknown col>'}) and ${r.type.name} (${r.id ?? '<unknown col>'}) cannot be matched`);
        }
        cols[i] = {
            name: l.id ?? ('column' + i),
            type,
            lval: l.cast(type),
            rval: r.cast(type),
        };
    }
    return cols;
}

export function buildUnion(left: _ISelection, right: _ISelection) {
    return new Union(reconcileSetOpCols(left, right, 'UNION'), left, right);
}

export function buildSetOp(left: _ISelection, right: _ISelection, op: 'intersect' | 'except', all: boolean) {
    const cols = reconcileSetOpCols(left, right, op.toUpperCase());
    return new SetOp(cols, left, right, op, all);
}

interface UCol {
    name: string;
    type: _IType;
    lval: IValue;
    rval: IValue;
}

class Union extends DataSourceBase {

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    isAggregation(): this is _IAggregation {
        return false;
    }

    readonly columns: ReadonlyArray<IValue>;
    private readonly colsByName = new Map<string, IValue>();

    entropy(t: _Transaction) {
        return this.left.entropy(t) + this.right.entropy(t);
    }

    hasItem(raw: Row, t: _Transaction): boolean {
        return this.left.hasItem(raw, t) || this.right.hasItem(raw, t);
    }

    constructor(private cols: UCol[]
        , private left: _ISelection
        , private right: _ISelection) {
        super(left.ownerSchema);
        this.columns = cols.map(x => columnEvaluator(this, x.name, x.type));
        for (const c of this.columns) {
            this.colsByName.set(c.id!, c);
        }
    }

    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        for (const raw of this.left.enumerate(t)) {
            const ret = {} as any;
            setId(ret, getId(raw));
            for (const c of this.cols) {
                ret[c.name] = c.lval.get(raw, t);
            }
            yield ret;
        }
        for (const raw of this.right.enumerate(t)) {
            const ret = {} as any;
            setId(ret, getId(raw));
            for (const c of this.cols) {
                ret[c.name] = c.rval.get(raw, t);
            }
            yield ret;
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'union',
            union: [this.left.explain(e),
            this.right.explain(e)],
        };
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        return colByName(this.colsByName, column, nullIfNotFound);
    }

    getIndex(...forValue: IValue[]): _IIndex | null | undefined {
        // todo use indices on unions
        return null;
    }

    isOriginOf(a: IValue): boolean {
        return a.origin === this || this.left.isOriginOf(a);
    }
}


/** INTERSECT / EXCEPT, with multiset (ALL) semantics. NULLs compare equal. */
class SetOp extends DataSourceBase {

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    isAggregation(): this is _IAggregation {
        return false;
    }

    readonly columns: ReadonlyArray<IValue>;
    private readonly colsByName = new Map<string, IValue>();

    constructor(private cols: UCol[]
        , private left: _ISelection
        , private right: _ISelection
        , private op: 'intersect' | 'except'
        , private all: boolean) {
        super(left.ownerSchema);
        this.columns = cols.map(x => columnEvaluator(this, x.name, x.type));
        for (const c of this.columns) {
            this.colsByName.set(c.id!, c);
        }
    }

    entropy(t: _Transaction) {
        return this.left.entropy(t) + this.right.entropy(t);
    }

    hasItem(raw: Row, t: _Transaction): boolean {
        return false;
    }

    stats(t: _Transaction): Stats | null {
        return null;
    }

    private key(row: Row, t: _Transaction, side: 'l' | 'r'): string {
        return JSON.stringify(this.cols.map(c => {
            const v = (side === 'l' ? c.lval : c.rval).get(row, t);
            if (v === null || v === undefined) { return null; }
            if (v instanceof Date) { return ['@d', v.getTime()]; }
            return v;
        }));
    }

    private normalized(row: Row, t: _Transaction): Row {
        const ret = {} as any;
        setId(ret, getId(row));
        for (const c of this.cols) {
            ret[c.name] = c.lval.get(row, t);
        }
        return ret;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        // count keys on the right
        const rightCounts = new Map<string, number>();
        for (const r of this.right.enumerate(t)) {
            const k = this.key(r, t, 'r');
            rightCounts.set(k, (rightCounts.get(k) ?? 0) + 1);
        }

        const emitted = new Map<string, number>();
        for (const l of this.left.enumerate(t)) {
            const k = this.key(l, t, 'l');
            const inRight = rightCounts.get(k) ?? 0;
            const already = emitted.get(k) ?? 0;

            let emit: boolean;
            if (this.op === 'intersect') {
                emit = this.all
                    ? inRight > already        // up to min(left, right) copies
                    : inRight > 0 && already === 0;
            } else {
                emit = this.all
                    ? already >= inRight       // skip the first `inRight` copies
                    : inRight === 0 && already === 0;
            }
            if (emit) {
                yield this.normalized(l, t);
            }
            emitted.set(k, already + 1);
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'union',
            union: [this.left.explain(e), this.right.explain(e)],
        };
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        return colByName(this.colsByName, column, nullIfNotFound);
    }

    getIndex(...forValue: IValue[]): _IIndex | null | undefined {
        return null;
    }

    isOriginOf(a: IValue): boolean {
        return a.origin === this || this.left.isOriginOf(a);
    }
}
