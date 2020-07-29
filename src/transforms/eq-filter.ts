import { _ISelection, IValue, _IIndex, _ITable, _Transaction, _Explainer, _SelectExplanation, IndexKey, IndexOp, Stats } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { nullIsh } from '../utils';

export class EqFilter<T = any> extends FilterBase<T> {

    private index: _IIndex;
    private opDef: IndexOp;

    entropy(t: _Transaction): number {
        return this.index.entropy({ ...this.opDef, t });
    }


    stats(t: _Transaction): Stats | null {
        const stats = this.index.stats(t, [this.equalsCst]);
        if (this.op === 'eq' || !stats) {
            return stats;
        }
        // if neq, then compute from eq and all
        const all = this.index.stats(t);
        if (!all) {
            return null;
        }
        return {
            count: all.count - stats.count,
        };
    }

    hasItem(item: T, t: _Transaction) {
        const val = this.onValue.get(item, t);
        if (nullIsh(val)) {
            return false;
        }
        const eq = this.onValue.type.equals(val, this.equalsCst);
        if (nullIsh(eq)) {
            return false;
        }
        return this.op === 'eq' ? eq : !eq;
    }

    constructor(private onValue: IValue<T>
        , private equalsCst: any
        , private op: 'eq' | 'neq'
        , private matchNull: boolean) {
        super(onValue.origin);
        if (onValue.index.expressions.length !== 1) {
            throw new Error('Unexpected index equality expressions count mismatch');
        }

        this.index = this.onValue.index;
        this.opDef = {
            type: op,
            key: [equalsCst],
            t: null,
            matchNull: this.matchNull,
        }
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this.index.enumerate({ ...this.opDef, t })) {
            yield item;
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: this.op,
            entropy: this.entropy(e.transaction),
            on: this.onValue.index.explain(e),
        };
    }
}
