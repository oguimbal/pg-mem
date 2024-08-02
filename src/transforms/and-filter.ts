import { _ISelection, _IIndex, _ITable, _Transaction, _Explainer, _SelectExplanation, Stats, nil, Row } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { SeqScanFilter } from './seq-scan';



export class AndFilter extends FilterBase {

    get index(): _IIndex | nil {
        return null;
    }

    private prevEntropy?: { t: _Transaction; ret: number };

    entropy(t: _Transaction) {
        // just a bit of caching
        if (t === this.prevEntropy?.t) {
            return this.prevEntropy.ret;
        }
        const { best } = this.plan(t);
        const ret = best.entropy(t);
        this.prevEntropy = {
            ret,
            t,
        };
        return ret;
    }

    hasItem(value: Row, t: _Transaction): boolean {
        return this.filters.every(x => x.hasItem(value, t));
    }

    constructor(private filters: _ISelection[]) {
        super(filters.find(x => !(x instanceof SeqScanFilter)) ?? filters[0]);
        if (filters.some(f => f.columns !== this.base.columns)) {
            throw new Error('Column set mismatch');
        }
    }

    private plan(t: _Transaction) {
        const sorted = [...this.filters]
            .sort((a, b) => a.entropy(t) > b.entropy(t) ? 1 : -1);
        const [best] = sorted.splice(0, 1);
        return { best, sorted };
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        // sort them so the most restrictive filter is first
        const { best, sorted } = this.plan(t)
        for (const item of best.enumerate(t)) {
            if (!sorted.every(x => x.hasItem(item, t))) {
                continue;
            }
            yield item;
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        const { best, sorted } = this.plan(e.transaction);
        return {
            id: e.idFor(this),
            _: 'and',
            enumerate: best.explain(e),
            andCheck: sorted.map(x => x.explain(e)),
        };
    }

}