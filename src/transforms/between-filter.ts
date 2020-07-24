import { _ISelection, IValue, _IIndex, _ITable, _Transaction, _Explainer, _SelectExplanation } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { nullIsh } from '../utils';

export class BetweenFilter<T = any> extends FilterBase<T> {

    entropy(t: _Transaction) {
        return this.onValue.index.entropy(t);
    }

    hasItem(item: T, t: _Transaction) {
        return !this.onValue.index.hasItem(item, t);
    }

    constructor(private onValue: IValue<T>
        , private lo: any
        , private hi: any) {
        super(onValue.origin);
        if (onValue.index.expressions[0] !== onValue) {
            throw new Error('Between index misuse');
        }
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this.onValue.index.ge([this.lo], t)) {
            const tv = this.onValue.get(item, t);
            if (nullIsh(tv) || this.onValue.type.gt(tv, this.hi)) {
                break;
            }
            yield item;
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            type: 'inside',
            on: this.onValue.index.explain(e),
        };
    }
}


export class NotBetweenFilter<T = any> extends FilterBase<T> {

    entropy(t: _Transaction) {
        return this.onValue.index.entropy(t);
    }

    hasItem(item: T, t: _Transaction) {
        return !this.onValue.index.hasItem(item, t);
    }

    constructor(private onValue: IValue<T>
        , private lo: any
        , private hi: any) {
        super(onValue.origin);
        if (onValue.index.expressions[0] !== onValue) {
            throw new Error('Between index misuse');
        }
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this.onValue.index.lt([this.lo], t)) {
            const tv = this.onValue.get(item, t);
            if (nullIsh(tv)) {
                continue;
            }
            yield item;
        }
        for (const item of this.onValue.index.gt([this.hi], t)) {
            const tv = this.onValue.get(item, t);
            if (nullIsh(tv)) {
                break;
            }
            yield item;
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            type: 'outside',
            on: this.onValue.index.explain(e),
        };
    }
}