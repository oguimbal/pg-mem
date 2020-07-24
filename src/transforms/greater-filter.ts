import { _ISelection, IValue, _IIndex, _ITable, _Transaction, _Explainer, _SelectExplanation } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { nullIsh } from '../utils';

export class GreaterFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        return this.onValue.index.entropy(t);
    }

    hasItem(item: T, t: _Transaction) {
        const val = this.onValue.get(item, t);
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

    *enumerate(t: _Transaction): Iterable<T> {
        const index = this.onValue.index;
        for (const item of index[this.op]([this.than], t)) {
            const got = this.onValue.get(item, t);
            if (nullIsh(got) || !this.onValue.type[this.op](got, this.than)) {
                break;
            }
            yield item;
        }
    }


    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            type: 'ineq',
            on: this.onValue.index.explain(e),
        };
    }
}