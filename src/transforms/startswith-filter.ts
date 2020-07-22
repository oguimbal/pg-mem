import { _ISelection, IValue, _IIndex, _ITable, getId } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { DataType, CastError, QueryError } from '../interfaces';

export class StartsWithFilter<T = any> extends FilterBase<T> {

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
        , private startWith: string) {
        super(onValue.origin);
        if (onValue.index.expressions[0] !== this.onValue) {
            throw new Error('Startwith must be the first component of the index');
        }
    }

    *enumerate(): Iterable<T> {
        const index = this.onValue.index;
        for (const item of index.ge([this.startWith])) {
            const got: string = this.onValue.get(item);
            if (got === null || !got.startsWith(this.startWith)) {
                break;
            }
            yield item;
        }
    }
}