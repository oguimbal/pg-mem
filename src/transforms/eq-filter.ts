import { _ISelection, IValue, _IIndex, _ITable, _Transaction, _Explainer, _SelectExplanation } from '../interfaces-private';
import { FilterBase } from './transform-base';

export class EqFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        return this.onValue.index.entropy(t);
    }

    hasItem(item: T, t: _Transaction) {
        return this.onValue.index.hasItem(item, t);
    }

    constructor(private onValue: IValue<T>
        , private other: IValue[]) {
        super(onValue.origin);
        if (onValue.index.expressions.length !== other.length) {
            throw new Error('Unexpected index equality expressions count mismatch');
        }
        for (const o of other) {
            if (!o.isConstant) {
                throw new Error('Unexpected error: Index is being compared to a non constant');
            }
        }
    }

    *enumerate(t: _Transaction): Iterable<T> {
        const index = this.onValue.index;
        const map = index.expressions.map((v, i) => {
            const otherConv = this.other[i].convert(v.type);
            return otherConv.get();
        });
        for (const item of index.eq(map, t)) {
            yield item;
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            type: 'eq',
            on: this.onValue.index.explain(e),
        };
    }
}