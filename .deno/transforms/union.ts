import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats, _IIndex, _IType, setId, getId } from '../interfaces-private.ts';
import { DataSourceBase } from './transform-base.ts';
import { ColumnNotFound, nil, NotSupported, QueryError } from '../interfaces.ts';
import { columnEvaluator } from './selection.ts';
import { reconciliateTypes } from '../datatypes/datatypes.ts';
import { ExprRef } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { colByName } from '../utils.ts';

// https://www.postgresql.org/docs/current/typeconv-union-case.html
export function buildUnion(left: _ISelection, right: _ISelection) {
    if (left.columns.length !== right.columns.length) {
        throw new QueryError('each UNION query must have the same number of columns');
    }
    const cols: UCol[] = Array(left.columns.length);
    for (let i = 0; i < left.columns.length; i++) {
        const l = left.columns[i];
        const r = right.columns[i];

        const type = reconciliateTypes([l, r], true);
        if (!type) {
            throw new QueryError(`UNION types ${l.type.name} and ${r.type.name} cannot be matched`);
        }
        cols[i] = {
            name: l.id ?? ('column' + i),
            type,
            lval: l.cast(type),
            rval: r.cast(type),
        };
    }
    return new Union(cols, left, right);
}

interface UCol {
    name: string;
    type: _IType;
    lval: IValue;
    rval: IValue;
}

class Union<T = any> extends DataSourceBase<T> {

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    isAggregation() {
        return false;
    }

    readonly columns: ReadonlyArray<IValue<any>>;
    private readonly colsByName = new Map<string, IValue>();

    entropy(t: _Transaction) {
        return this.left.entropy(t) + this.right.entropy(t);
    }

    hasItem(raw: T, t: _Transaction): boolean {
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

    *enumerate(t: _Transaction): Iterable<T> {
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
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue<any> | nil {
        return colByName(this.colsByName, column, nullIfNotFound);
    }

    getIndex(...forValue: IValue<any>[]): _IIndex<any> | null | undefined {
        // todo use indices on unions
        return null;
    }

    isOriginOf(a: IValue<any>): boolean {
        return a.origin === this || this.left.isOriginOf(a);
    }
}
