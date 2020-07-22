import { _ISelection, IValue, _IIndex, _ITable } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { nullIsh } from '../utils';

export class BetweenFilter<T = any> extends FilterBase<T> {

    get entropy() {
        return this.onValue.index.entropy;
    }

    hasItem(item: T) {
        return !this.onValue.index.hasItem(item);
    }

    constructor(private onValue: IValue<T>
        , private lo: any
        , private hi: any) {
        super(onValue.origin);
        if (onValue.index.expressions[0] !== onValue) {
            throw new Error('Between index misuse');
        }
    }

    *enumerate(): Iterable<T> {
        for (const item of this.onValue.index.ge([this.lo])) {
            const tv = this.onValue.get(item);
            if (nullIsh(tv) || this.onValue.type.gt(tv, this.hi)) {
                break;
            }
            yield item;
        }
    }
}


export class NotBetweenFilter<T = any> extends FilterBase<T> {

    get entropy() {
        return this.onValue.index.entropy;
    }

    hasItem(item: T) {
        return !this.onValue.index.hasItem(item);
    }

    constructor(private onValue: IValue<T>
        , private lo: any
        , private hi: any) {
        super(onValue.origin);
        if (onValue.index.expressions[0] !== onValue) {
            throw new Error('Between index misuse');
        }
    }

    *enumerate(): Iterable<T> {
        for (const item of this.onValue.index.lt([this.lo])) {
            const tv = this.onValue.get(item);
            if (nullIsh(tv)) {
                continue;
            }
            yield item;
        }
        for (const item of this.onValue.index.gt([this.hi])) {
            const tv = this.onValue.get(item);
            if (nullIsh(tv)) {
                break;
            }
            yield item;
        }
    }
}