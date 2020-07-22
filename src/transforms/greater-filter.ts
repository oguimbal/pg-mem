import { _ISelection, IValue, _IIndex, _ITable } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { nullIsh } from '../utils';

export class GreaterFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    get entropy() {
        return this.onValue.index.entropy;
    }

    hasItem(item: T) {
        const val = this.onValue.get(item);
        if (nullIsh(val)) {
            return false;
        }
        return this.onValue.type[this.op](this.than, val);
    }

    constructor(private onValue: IValue<T>
        , private op: 'gt' | 'ge' | 'lt' | 'le'
        , private than: any) {
        super(onValue.origin);
        if (onValue.index.expressions[0] !== onValue) {
            throw new Error('Can only filter on first column of index');
        }
    }

    *enumerate(): Iterable<T> {
        const index = this.onValue.index;
        for (const item of index[this.op]([this.than])) {
            const got = this.onValue.get(item);
            if (nullIsh(got) || !this.onValue.type[this.op](got, this.than)) {
                break;
            }
            yield item;
        }
    }
}