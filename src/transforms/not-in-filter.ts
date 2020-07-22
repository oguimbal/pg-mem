import { _ISelection, IValue, _IIndex, _ITable, getId } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { DataType, CastError, QueryError } from '../interfaces';

export class NotInFilter<T = any> extends FilterBase<T> {

    get entropy() {
        return this.onValue.index.entropy;
    }

    hasItem(item: T): boolean {
        // return this.onValue.index.hasItem(item);
        throw new Error('todo');
    }

    constructor(private onValue: IValue<T>
        , private elts: any[]) {
        super(onValue.origin);
        if (onValue.index.expressions.length !== 1) {
            throw new Error('Only supports IN with signle expressions index');
        }
        if (!Array.isArray(elts)) {
            throw new QueryError('Cannot iterate element list');
        }
    }

    *enumerate(): Iterable<T> {
        const index = this.onValue.index;
        if (!Array.isArray(this.elts)) {
            throw new QueryError('Cannot iterate element list');
        }
        for (const item of index.nin(this.elts.map(x => [x]))) {
            yield item;
        }
    }
}