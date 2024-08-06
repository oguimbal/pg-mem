import { _ISelection, IValue, _IIndex, _ITable, getId, _Transaction, _Explainer, _SelectExplanation, Stats, Row } from '../interfaces-private.ts';
import { FilterBase } from './transform-base.ts';
import { nullIsh } from '../utils.ts';

export class StartsWithFilter extends FilterBase {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        return this.onValue.index!.entropy({
            type: 'ge',
            key: [this.startWith],
            t,
        });
    }

    hasItem(item: Row, t: _Transaction) {
        const get = this.onValue.get(item, t);
        return typeof get === 'string'
            && get.startsWith(this.startWith);
    }

    constructor(private onValue: IValue
        , private startWith: string) {
        super(onValue.origin!);
        if (onValue.index!.expressions[0].hash !== this.onValue.hash) {
            throw new Error('Startwith must be the first component of the index');
        }
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        const index = this.onValue.index!;
        for (const item of index.enumerate({
            type: 'ge',
            key: [this.startWith],
            t,
        })) {
            const got: string = this.onValue.get(item, t);
            if (nullIsh(got) || !got.startsWith(this.startWith)) {
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