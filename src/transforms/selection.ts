import { _ISelection, _IIndex, IValue, _ISelectionSource, setId, getId, _IType } from '../interfaces-private';
import { QueryError, ColumnNotFound, DataType, CastError, Schema } from '../interfaces';
import { buildValue } from '../predicate';
import { NotSupported, buildColumnIds } from '../utils';
import { Evaluator } from '../valuetypes';
import { TransformBase } from './transform-base';

export function buildSelection(on: _ISelection, select: any[] | '*') {
    if (select === '*') {
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
    private columnIds: string[];

    get columns(): IValue[] {
        return this._columns;
    }
    private columnsById: { [key: string]: IValue } = {};


    constructor(base: _ISelectionSource<any>, opts: {
        schema?: Schema;
        columns?: any[];
        alias?: string;
    }) {
        super(base);
        if (opts.columns) {
            if (!opts.columns.length) {
                throw new QueryError('Invalid selection');
            }
            for (const s of opts.columns) {
                let col = buildValue(base as _ISelection, s.expr);
                if (s.as) {
                    col = col.setId(s.as);
                }
                this._columns.push(col);
                this.columnsById[col.id] = col;
            }
            this.alias = 'selection:' + (selCnt++);
        } else if (opts.schema) {
            this.alias = opts.schema.name;
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
                this.columnsById[newCol.id] = newCol;
            }
        } else if (opts.alias) {
            const asSel = base as _ISelection;
            if (!asSel.columns) { // istanbul ignore next
                throw new Error('Should only apply aliases on actual selection');
            }
            this._columns = asSel.columns;
            this.alias = opts.alias;
            for (const k of this.columns) {
                this.columnsById[k.id] = k;
            }
        } else {
            throw new NotSupported('selection does nothing');
        }


        this.columnIds = buildColumnIds(this.columns);
    }

    *enumerate(): Iterable<T> {
        for (const item of this.base.enumerate()) {
            const ret = {};
            setId(ret, getId(item) ?? (this.alias + getId(item)));
            for (let i = 0; i < this.columns.length; i++) {
                const col = this.columns[i];
                ret[this.columnIds[i]] = col.get(item) ?? null;
            }
            yield ret as any;
        }
    }


    getColumn(column: string, nullIfNotFound?: boolean): IValue {
        const exec = /^([^.]+)\.(.+)$/.exec(column);
        if (exec) {
            if (exec[1] !== this.alias) {
                if (nullIfNotFound) {
                    return null;
                }
                throw new QueryError(`Alias '${exec[1]}' not found`)
            }
            column = exec[2];
        }
        const ret = this.columnsById[column];
        if (!ret) {
            if (nullIfNotFound) {
                return null;
            }
            throw new ColumnNotFound(column);
        }
        return ret;
    }
}
