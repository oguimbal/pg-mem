import { _ISelection, _IIndex, IValue, setId, getId, _IType, _Transaction, _Column, _ITable, _Explainer, _SelectExplanation, IndexKey, _IndexExplanation, IndexExpression, IndexOp } from '../interfaces-private';
import { QueryError, ColumnNotFound, DataType, CastError, Schema, NotSupported, AmbiguousColumn, SchemaField } from '../interfaces';
import { buildValue } from '../predicate';
import { buildColumnIds } from '../utils';
import { Evaluator } from '../valuetypes';
import { TransformBase } from './transform-base';
import { SelectedColumn, CreateColumnDef } from '../parser/syntax/ast';

export function buildSelection(on: _ISelection, select: SelectedColumn[]) {
    const [first] = select;
    if (select.length === 1 && first.expr.type === 'ref' && first.expr.name === '*' && !first.expr.table) {
        if (!on.columns.length) {
            throw new QueryError('SELECT * with no tables specified is not valid');
        }
        return on;
    }
    return new Selection(on, select);
}

export function columnEvaluator(this: void, on: _ISelection, id: string, type: _IType) {
    if (!id) {
        throw new Error('Invalid column id');
    }
    const ret = new Evaluator(
        type
        , id
        , id
        , id
        , null
        , raw => raw[id]
        , {
            isColumnOf: on,
        });
    return ret;
}


export class Selection<T> extends TransformBase<T> implements _ISelection<T> {

    private columnIds: string[] = [];
    private columnsOrigin: IValue[] = [];
    private columnMapping = new Map<IValue, IValue>();
    private indexCache = new Map<IValue, _IIndex>();
    private columnsById: { [key: string]: IValue[] } = {};
    private symbol = Symbol();

    readonly columns: IValue[] = [];


    constructor(base: _ISelection<any>, columns: SelectedColumn[]) {
        super(base);

        if (!columns.length) {
            throw new QueryError('Invalid selection');
        }

        // build non-conflicting column ids based on existing ones
        this.columnIds = [];
        for (const s of columns) {
            if (s.expr.type === 'ref') {
                if (s.expr.name === '*') {
                    if (s.alias) {
                        throw new QueryError('Cannot alias *');
                    }
                    for (const _col of base.columns) {
                        this.columnIds.push(_col.id);
                    }
                } else {
                    this.columnIds.push(s.alias ?? s.expr.name);
                }
            } else {
                this.columnIds.push(s.alias);
            }
        }
        this.columnIds = buildColumnIds(this.columnIds);

        // build columns to select
        for (let i = 0; i < columns.length; i++) {
            const s = columns[i];
            if (s.expr.type === 'ref' && s.expr.name === '*') {
                for (const _col of base.columns) {
                    this.refColumn(_col, this.columnIds[i]);
                }
            } else {
                let _col = buildValue(base as _ISelection, s.expr);
                this.refColumn(_col, this.columnIds[i]);
            }
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
        const low = col.id.toLowerCase();
        let ci = this.columnsById[low];
        if (!ci) {
            this.columnsById[low] = ci = [];
        }
        ci.push(col);
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this.base.enumerate(t)) {
            yield this.build(item, t);
        }
    }

    build(item: any, t: _Transaction): T {
        const ret = {};
        setId(ret, getId(item));
        ret[this.symbol] = this.symbol;
        for (let i = 0; i < this.columns.length; i++) {
            const col = this.columnsOrigin[i];
            ret[this.columnIds[i]] = col.get(item, t) ?? null;
        }
        return ret as any;
    }

    hasItem(value: T, t: _Transaction): boolean {
        return value[this.symbol] === this.symbol;
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue {
        const ret = this.columnsById[column.toLowerCase()];
        if (!ret?.length) {
            if (nullIfNotFound) {
                return null;
            }
            throw new ColumnNotFound(column);
        }
        if (ret.length !== 1) {
            throw new AmbiguousColumn(column);
        }
        return ret[0];
    }

    getIndex(val: IValue) {
        if (this.indexCache.has(val)) {
            return this.indexCache.get(val);
        }
        const mapped = this.columnMapping.get(val);
        const originIndex = this.base.getIndex(mapped);
        const ret = originIndex && new SelectionIndex(this, originIndex);
        this.indexCache.set(val, ret);
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