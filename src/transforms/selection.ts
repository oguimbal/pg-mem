import { _ISelection, _IIndex, IValue, setId, getId, _IType, _Transaction, _Column, _ITable, _Explainer, _SelectExplanation } from '../interfaces-private';
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

    private _columns: IValue[] = [];
    private columnIds: string[] = [];
    private columnsById: { [key: string]: IValue[] } = {};

    get columns(): IValue[] {
        return this._columns;
    }


    constructor(base: _ISelection<any>, columns: SelectedColumn[]) {
        super(base);

        if (!columns.length) {
            throw new QueryError('Invalid selection');
        }

        // build columns to select
        for (const s of columns) {
            if (s.expr.type === 'ref' && s.expr.name === '*') {
                if (s.alias) {
                    throw new QueryError('Cannot alias *');
                }
                for (const _col of (base as _ISelection).columns) {
                    this._columns.push(_col);
                    this.refColumn(_col);
                }
            } else {
                let col = buildValue(base as _ISelection, s.expr);
                if (s.alias) {
                    col = col.setId(s.alias);
                }
                this._columns.push(col);
                this.refColumn(col);
            }
        }


        this.columnIds = buildColumnIds(this.columns);
    }

    addColumn(col: IValue) {
        this._columns.push(col);
        this.refColumn(col);
    }

    private refColumn(col: IValue<any>) {
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
            const ret = {};
            setId(ret, getId(item));
            for (let i = 0; i < this.columns.length; i++) {
                const col = this.columns[i];
                ret[this.columnIds[i]] = col.get(item, t) ?? null;
            }
            yield ret as any;
        }
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

    explain(e: _Explainer): _SelectExplanation {


        return {
            id: e.idFor(this),
            type: 'map',
            of: this.base.explain(e),
            select: this.columnIds.map((x, i) => ({
                what: this.columns[i].explain(e),
                as: x
            })),
        };
    }
}
