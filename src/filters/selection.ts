import { _ISelection, _IIndex, IValue, BuildState, _ISelectionSource } from '../interfaces-private';
import { QueryError, ColumnNotFound, DataType, CastError, Schema } from '../interfaces';
import { buildValue } from '../predicate';
import { trimNullish, NotSupported } from '../utils';
import { EqFilter } from './eq-filter';
import { BoolValue } from '../datatypes';
import { FalseFilter } from './false-filter';
import { buildAndFilter } from './and-filter';
import { OrFilter } from './or-filter';
import { SeqScanFilter } from './seq-scan';
import { Column } from '../column';

export function buildSelection(on: _ISelection, select: any[] | '*') {
    if (select === '*') {
        return on;
    }
    return new Selection(on, select);
}



export class Selection<T> implements _ISelection<T> {
    // readonly index: _IIndex<T>; // <== ??

    get entropy(): number {
        return this.base.entropy;
    }

    hasItem(value: T): boolean {
        return this.base.hasItem(value);
    }

    columns: IValue[] = [];
    private columnsById: { [key: string]: IValue } = {};


    constructor(private base: _ISelectionSource<any>, what: any[] | Schema) {
        if (Array.isArray(what)) {
            if (!what.length) {
                throw new QueryError('Invalid selection');
            }
            for (const s of what) {
                debugger;
                const id = null;  // <== what ? 'as' ?
                const col = buildValue(this, s).setId(id);
                this.columns.push(col);
                this.columnsById[col.id] = col;
            }
        } else {
            for (const col of what.fields) {
                const newCol = new Column(this, col);
                this.columns.push(newCol.value);
                this.columnsById[newCol.value.id] = newCol.value;
            }
        }
    }


    sql(state: BuildState) {
        state = state ?? { alias: 0 };
        state.alias++;
        const alias = 'a' + state.alias;
        return `(SELECT ${this.columns.map(x => alias + '.' + x.id)} FROM ${this.base.sql(state)} ${alias})`;
    }

    *enumerate(): Iterable<T> {
        for (const item of this.base.enumerate()) {
            const ret = {};
            for (const col of this.columns) {
                ret[col.id] = col.get(item);
            }
            yield ret as any;
        }
    }

    getColumn(column: string): IValue {
        const ret = this.columnsById[column];
        if (!ret) {
            throw new ColumnNotFound(column);
        }
        return ret;
    }


    select(select: any[] | "*"): _ISelection<any> {
        return buildSelection(this, select);
    }

    filter(filter: any): _ISelection {
        if (!filter) {
            return this;
        }
        filter = trimNullish(filter);
        const plan = this.buildFilter(filter);
        return plan;
    }

    private buildFilter(filter: any): _ISelection<T> {

        // check if there is a direct index
        const built = buildValue(this, filter);
        if (built.index) {
            if (built.index.expressions.length !== 1) {
                throw new Error('Was not expecing multiples expressions filter');
            }
            const itype = built.index.expressions[0].type;
            if (itype !== DataType.bool) {
                throw new CastError(itype, DataType.bool);
            }
            return new EqFilter(built, [BoolValue.constant(true)]);
        }

        // if this filter is a constant expression (ex: 1 = 1)
        // then return directly
        if (built.isConstant) {
            const val = built.convert(DataType.bool)
                .get(null);
            if (val) {
                return this;
            }
            return new FalseFilter(this);
        }
        switch (filter.type) {
            case 'binary_expr':
                return this.buildBinaryFilter(filter);
            default:
                throw new NotSupported('condition ' + filter.type);
        }
    }

    private buildBinaryFilter(filter: any): _ISelection<T> {
        const { left, right, operator } = filter;
        switch (operator) {
            case '=':
            case '>':
            case '<':
            case '<=':
            case '>=':
                return this.buildComparison(filter);
            case 'AND':
            case 'OR':
                const leftFilter = this.buildFilter(left);
                const rightFilter = this.buildFilter(right);
                return operator === 'AND'
                    ? buildAndFilter(leftFilter, rightFilter)
                    : new OrFilter(leftFilter, rightFilter);
            default:
                return new SeqScanFilter(this, buildValue(this, filter));
        }
    }

    private buildComparison(filter: any): _ISelection<T> {
        const { operator, left, right } = filter;
        let leftValue = buildValue(this, left);
        let rightValue = buildValue(this, right);

        if (leftValue.isConstant && rightValue.isConstant) {
            throw new Error('Was not expecting constants on both sides of comparison');
        }

        if (operator === '=') {
            if (leftValue.index && rightValue.isConstant) {
                return new EqFilter(leftValue, [rightValue]);
            }
            if (rightValue.index && leftValue.isConstant) {
                return new EqFilter(rightValue, [leftValue]);
            }
        }

        return new SeqScanFilter(this, buildValue(this, filter));
    }

    getIndex(forValue: IValue): _IIndex<any> {
        return this.base.getIndex(forValue);
    }
}