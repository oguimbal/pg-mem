import { _ISelection, IValue, _IIndex, _ITable, _Transaction, _Explainer, _SelectExplanation, IndexOp, Stats, Row } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { nullIsh } from '../utils';

export class BetweenFilter extends FilterBase {

    private opDef: IndexOp;


    entropy(t: _Transaction) {
        return this.onValue.index!.entropy({ ...this.opDef, t });
    }

    constructor(private onValue: IValue
        , private lo: any
        , private hi: any
        , private op: 'inside' | 'outside') {
        super(onValue.origin!);
        if (onValue.index!.expressions[0]?.hash !== onValue.hash) {
            throw new Error('Between index misuse');
        }
        this.opDef = {
            type: op,
            hi: [hi],
            lo: [lo],
            t: null as any,
        }
    }

    hasItem(value: Row, t: _Transaction): boolean {
        const v = this.onValue.get(value, t);
        if (nullIsh(v)) {
            return false;
        }
        if (this.op === 'inside') {
            return !!this.onValue.type.ge(v, this.lo)
                && !!this.onValue.type.le(v, this.hi);
        }
        return !!this.onValue.type.lt(v, this.lo)
            || !!this.onValue.type.gt(v, this.lo);
    }

    enumerate(t: _Transaction): Iterable<Row> {
        return this.onValue.index!.enumerate({ ...this.opDef, t });
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: this.op,
            entropy: this.entropy(e.transaction),
            on: this.onValue.index!.explain(e),
        };
    }
}
