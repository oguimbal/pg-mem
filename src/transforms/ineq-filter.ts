import { _ISelection, IValue, _IIndex, _ITable, _Transaction, _Explainer, _SelectExplanation, IndexOp, Stats } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { nullIsh } from '../utils';

export class IneqFilter<T = any> extends FilterBase<T> {

    private index: _IIndex;
    private opDef: IndexOp;

    entropy(t: _Transaction) {
        return this.onValue.index!.entropy({ ...this.opDef, t });
    }

    hasItem(item: T, t: _Transaction) {
        const val = this.onValue.get(item, t);
        if (nullIsh(val)) {
            return false;
        }
        return !!this.onValue.type[this.op](val, this.than);
    }

    constructor(private onValue: IValue<T>
        , private op: 'gt' | 'ge' | 'lt' | 'le'
        , private than: any) {
        super(onValue.origin!);

        this.index = this.onValue.index!;
        this.opDef = {
            type: op,
            key: [than],
            t: null as any,
        }
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const item of this.index.enumerate({ ...this.opDef, t })) {
            if (!this.hasItem(item, t)) {
                break;
            }
            yield item;
        }
    }


    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'ineq',
            entropy: this.entropy(e.transaction),
            on: this.onValue.index!.explain(e),
        };
    }
}