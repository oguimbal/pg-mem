import { _ISelection, _IIndex, IValue, setId, getId, _IType, _Transaction, _Column, _ITable, _Explainer, _SelectExplanation, IndexKey, _IndexExplanation, IndexExpression, IndexOp, Stats, _IAlias } from '../interfaces-private.ts';
import { QueryError, ColumnNotFound, AmbiguousColumn, nil } from '../interfaces.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { Evaluator } from '../evaluator.ts';
import { TransformBase } from './transform-base.ts';
import { SelectedColumn, Expr, astVisitor, ExprRef } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { aggregationFunctions, buildGroupBy } from './aggregation.ts';

import { asSingleQName, colByName, colToStr, isSelectAllArgList, suggestColumnName } from '../utils.ts';
import { withSelection } from '../parser/context.ts';


export function buildSelection(on: _ISelection, select: SelectedColumn[] | nil) {
    select = select ?? [];

    // if this is a "SELECT *" => just ignore
    if (isSelectAllArgList(select.map(x => x.expr))) {
        if (!on.columns.length) {
            throw new QueryError('SELECT * with no tables specified is not valid');
        }
        return on;
    }

    // if there is any aggregation function
    // check if there is any aggregation
    for (const col of select ?? []) {
        if (!on.isAggregation() && 'expr' in col && hasAggreg(col.expr)) {
            // yea, there is an aggregation somewhere in selection
            return buildGroupBy(on, []).select(select);
        }
    }

    return new Selection(on, select);
}


function hasAggreg(e: Expr) {
    let has = false;
    astVisitor(visitor => ({
        call: expr => {
            const nm = asSingleQName(expr.function, 'pg_catalog');
            if (nm && aggregationFunctions.has(nm)) {
                // yea, this is an aggregation
                has = true;
                return;
            }
            visitor.super().call(expr);
        }
    })).expr(e);
    return has
}



export function columnEvaluator(this: void, on: _ISelection, id: string, type: _IType) {
    if (!id) {
        throw new Error('Invalid column id');
    }
    const ret = new Evaluator(
        type
        , id
        , id
        , null
        , raw => raw[id]
        , {
            isColumnOf: on,
        });
    return ret;
}

function* buildCols(this: void, base: _ISelection, columns: (SelectedColumn | CustomAlias)[]): Iterable<CustomAlias> {
    for (const s of columns) {
        if ('val' in s) {
            if (s.val.origin !== base) {
                throw new Error('Corrupted selection');
            }
            yield s;
            continue;
        }
        if (s.expr.type === 'ref' && s.expr.name === '*') {
            // handle select "*"
            if (s.alias) {
                throw new QueryError('Cannot alias *');
            }
            let of: _IAlias = base;
            const alias = s.expr.table;
            if (alias) {
                // handle select "x.*"
                const sub = base.selectAlias(alias.name);
                if (!sub) {
                    throw new QueryError(`Unknown alias "${alias.name}"`);
                }
                of = sub;
            }

            for (const val of of.listColumns()) {
                yield { val };
            }

        } else {
            const val = buildValue(s.expr);
            yield { val, as: s.alias?.name, expr: s.expr };
        }
    }
}

export interface CustomAlias {
    val: IValue;
    as?: string;
    expr?: Expr
}

export class Selection<T = any> extends TransformBase<T> implements _ISelection<T> {

    private columnIds: string[] = [];
    private columnsOrigin: IValue[] = [];
    private columnMapping = new Map<IValue, IValue>();
    private indexCache = new Map<IValue, _IIndex>();
    private columnsById = new Map<string, IValue[]>();
    private symbol = Symbol();

    readonly columns: IValue[] = [];

    isAggregation() {
        return false;
    }


    constructor(base: _ISelection, _columns: (SelectedColumn | CustomAlias)[]) {
        super(base);

        // build non-conflicting column ids based on existing ones
        const columns = withSelection(base, () => [...buildCols(base, _columns)]);
        this.columnIds = columns.map(x => x.as ?? x.val.id!);

        // build column ids
        let anonymousBases = new Map<string, number>();
        for (let i = 0; i < this.columnIds.length; i++) {
            if (!this.columnIds[i]) {
                let id = suggestColumnName(columns[i].expr) ?? 'column';

                // check no collision with an existing column
                let cnt = anonymousBases.get(id);
                this.columnIds[i] = id + (cnt ? cnt : '');
                anonymousBases.set(id, (cnt ?? 0) + 1);
            }
        }


        // build columns to select
        for (let i = 0; i < columns.length; i++) {
            this.refColumn(columns[i].val, this.columnIds[i]);
        }


        // ONLY ONCE COLUMNS HAVE BEEN REFERENCED BY ID,
        // rename ids for columns which have the same id
        // this allows yielding ambiguous columns data
        const has = new Map<string, number>();
        for (let i = 0; i < columns.length; i++) {
            const orig = this.columnIds[i];
            const oi = has.get(orig);
            if (typeof oi !== 'number') {
                has.set(orig, i);
                continue;
            }
            let ret: string = orig;
            let k = 0;
            do {
                ret = orig + (++k);
            } while (this.columnIds.includes(ret));
            this.columnIds[i] = ret;
            has.set(ret, i);
        }
    }

    private refColumn(fromCol: IValue, alias: string) {
        const col = columnEvaluator(this, alias, fromCol.type);
        this.columns.push(col);
        this.columnMapping.set(col, fromCol);
        this.columnsOrigin.push(fromCol);
        if (!col.id) {
            return;
        }
        let ci = this.columnsById.get(col.id);
        if (!ci) {
            this.columnsById.set(col.id, ci = []);
        }
        ci.push(col);
    }


    stats(t: _Transaction): Stats | null {
        return this.base.stats(t);
    }


    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this.base.enumerate(t)) {
            yield this.build(item, t);
        }
    }

    build(item: any, t: _Transaction): T {
        const ret: any = {};
        setId(ret, getId(item));
        ret[this.symbol] = this.symbol;
        for (let i = 0; i < this.columns.length; i++) {
            const col = this.columnsOrigin[i];
            ret[this.columnIds[i]] = col.get(item, t) ?? null;
        }
        return ret as any;
    }

    hasItem(value: T, t: _Transaction): boolean {
        return (value as any)[this.symbol] === this.symbol;
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        const ret = colByName(this.columnsById, column, true);
        if (!ret?.length) {
            if (nullIfNotFound) {
                return null;
            }
            throw new ColumnNotFound(colToStr(column));
        }
        if (ret.length !== 1) {
            throw new AmbiguousColumn(colToStr(column));
        }
        return ret[0];
    }

    getIndex(val: IValue): _IIndex | nil {
        if (this.indexCache.has(val)) {
            return this.indexCache.get(val);
        }
        const mapped = this.columnMapping.get(val);
        const originIndex = this.base.getIndex(mapped!);
        const ret = originIndex && new SelectionIndex(this, originIndex);
        this.indexCache.set(val, ret!);
        return ret;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'map',
            of: this.base.explain(e),
            select: this.columnIds.map((x, i) => ({
                what: this.columnsOrigin[i].explain(e),
                as: x
            })),
        };
    }
}


export class SelectionIndex<T> implements _IIndex<T> {
    constructor(readonly owner: Selection<T>, private base: _IIndex) {
    }

    stats(t: _Transaction, key?: IndexKey) {
        return this.base.stats(t, key);
    }

    iterateKeys(t: _Transaction) {
        return this.base.iterateKeys(t);
    }

    get expressions(): IndexExpression[] {
        return this.base.expressions;
    }

    entropy(op: IndexOp): number {
        // same as source
        return this.base.entropy(op);
    }

    eqFirst(rawKey: IndexKey, t: _Transaction) {
        return this.base.eqFirst(rawKey, t);
    }

    *enumerate(op: IndexOp): Iterable<T> {
        for (const i of this.base.enumerate(op)) {
            yield this.owner.build(i, op.t);
        }
    }


    explain(e: _Explainer): _IndexExplanation {
        return {
            _: 'indexMap',
            of: this.base.explain(e),
        }
    }
}