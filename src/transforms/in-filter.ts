import { _ISelection, IValue, _IIndex, _ITable, getId } from '../interfaces-private';
import { FilterBase } from './filter-base';
import { DataType, CastError, QueryError } from '../interfaces';

export class InFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

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
        const array = this.elts.get(null);
        if (!Array.isArray(array)) {
            throw new QueryError('Cannot iterate element list');
        }
        const yielded = new Set<string>();
        for (const a of array) {
            for (const item of index.eq([a])) {
                const id = getId(item)
                if (yielded.has(id)) {
                    continue;
                }
                yield item;
                yielded.add(id);
            }
        }
    }
}