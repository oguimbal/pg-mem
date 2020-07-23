import { _ISelection, _IIndex, IValue, _ISelectionSource, setId, getId, _IType, _Transaction } from '../interfaces-private';
import { QueryError, ColumnNotFound, DataType, CastError, Schema, NotSupported, AmbiguousColumn } from '../interfaces';
import { buildValue } from '../predicate';
import { buildColumnIds } from '../utils';
import { Evaluator } from '../valuetypes';
import { TransformBase } from './transform-base';
import { SelectedColumn } from '../parser/syntax/ast';

export function buildSelection(on: _ISelection, select: SelectedColumn[]) {
    const [first] = select;
    if (select.length === 1 && first.expr.type === 'ref' && first.expr.name === '*' && !first.expr.table) {
        if (!on.columns.length) {
            throw new QueryError('SELECT * with no tables specified is not valid');
        }
        return on;
    }
    return new Selection(on, {
        columns: select,
    });
}

export function buildAlias(on: _ISelectionSource, alias?: string): _ISelection<any> {
    if (!alias) {
        return on as any;
    }
    return new Selection(on, {
        alias,
    });
}

let selCnt = 0;

export class Selection<T> extends TransformBase<T> implements _ISelection<T> {
    private alias: string;
    // readonly index: _IIndex<T>; // <== ??

    private _columns: IValue[] = [];
    private columnIds: string[] = [];

    get columns(): IValue[] {
        return this._columns;
    }
    private columnsById: { [key: string]: IValue[] } = {};


    constructor(base: _ISelectionSource<any>, opts: {
        schema?: Schema;
        columns?: SelectedColumn[];
        alias?: string;
    }) {
        super(base);
        if (opts.columns) {
            if (!opts.columns.length) {
                throw new QueryError('Invalid selection');
            }

            // build columns to select
            const cols: IValue[] = [];
            for (const s of opts.columns) {
                if (s.expr.type === 'ref' && s.expr.name === '*') {
                    if (s.alias) {
                        throw new QueryError('Cannot alias *');
                    }
                    cols.push(...(base as _ISelection).columns);
                } else {
                    let col = buildValue(base as _ISelection, s.expr);
                    if (s.alias) {
                        col = col.setId(s.alias);
                    }
                    cols.push(col);
                }
            }

            // push them
            for (const col of cols) {
                this._columns.push(col);
                this.refColumn(col);
            }
            this.alias = 'selection:' + (selCnt++);
        } else if (opts.schema) {
            this.alias = opts.schema.name?.toLowerCase();
            for (const _col of opts.schema.fields) {
                const col = _col;
                const newCol = new Evaluator(
                    col.type as _IType
                    , col.id
                    , col.id
                    , col.id
                    , this
                    , raw => raw[col.id])
                this._columns.push(newCol);
                this.refColumn(newCol);
            }
        } else if (opts.alias) {
            const asSel = base as _ISelection;
            if (!asSel.columns) { // istanbul ignore next
                throw new Error('Should only apply aliases on actual selection');
            }
            this._columns = asSel.columns;
            this.alias = opts.alias?.toLowerCase();
            for (const col of asSel.columns) {
                this.refColumn(col);
            }
        } else {
            throw new NotSupported('selection does nothing');
        }


        this.columnIds = buildColumnIds(this.columns);
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
            setId(ret, getId(item) ?? (this.alias + getId(item)));
            for (let i = 0; i < this.columns.length; i++) {
                const col = this.columns[i];
                ret[this.columnIds[i]] = col.get(item, t) ?? null;
            }
            yield ret as any;
        }
    }


    getColumn(column: string, nullIfNotFound?: boolean): IValue {
        const exec = /^([^.]+)\.(.+)$/.exec(column);
        if (exec) {
            if (exec[1].toLowerCase() !== this.alias) {
                if (nullIfNotFound) {
                    return null;
                }
                throw new QueryError(`Alias '${exec[1]}' not found`)
            }
            column = exec[2];
        }
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
}
