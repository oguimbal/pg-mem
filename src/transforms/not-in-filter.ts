import { _ISelection, IValue, _IIndex, _ITable, getId } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { DataType, CastError, QueryError } from '../interfaces';

export class NotInFilter<T = any> extends FilterBase<T> {

    get entropy() {
        return this.onValue.index.entropy;
    }

    hasItem(item: T) {
        return this.onValue.index.hasItem(item);
    }

    constructor(private onValue: IValue<T>
        , private elts: IValue) {
        super(onValue.origin);
        if (onValue.index.expressions.length !== 1) {
            throw new Error('Only supports IN with signle expressions index');
        }
        if (!elts.isConstant) {
            throw new Error('Unexpected error: Index is being compared to a non constant');
        }
        if (elts.type.primary !== DataType.array) {
            throw new CastError(elts.type.primary, DataType.array);
        }
    }

    *enumerate(): Iterable<T> {
        const index = this.onValue.index;
        let array = this.elts.get(null);
        if (!Array.isArray(array)) {
            throw new QueryError('Cannot iterate element list');
        }
        for (const item of index.nin(array.map(x => [x]))) {
            yield item;
        }
    }
}