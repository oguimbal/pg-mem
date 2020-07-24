import { _ISelection, IValue, _IIndex, _ITable, getId, _Transaction, _Explainer, _SelectExplanation } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { DataType, CastError, QueryError } from '../interfaces';

export class StartsWithFilter<T = any> extends FilterBase<T> {

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
        , private startWith: string) {
        super(onValue.origin);
        if (onValue.index.expressions[0] !== this.onValue) {
            throw new Error('Startwith must be the first component of the index');
        }
    }

    *enumerate(t: _Transaction): Iterable<T> {
        const index = this.onValue.index;
        for (const item of index.ge([this.startWith], t)) {
            const got: string = this.onValue.get(item, t);
            if (got === null || !got.startsWith(this.startWith)) {
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