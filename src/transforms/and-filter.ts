import { _ISelection, _IIndex, _ITable, _Transaction } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { SeqScanFilter } from './seq-scan';



export class AndFilter<T = any> extends FilterBase<T> {

    get index(): _IIndex<T> {
        return null;
    }

    private prevEntropy: { t: _Transaction; ret: number };

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

    hasItem(value: T, t: _Transaction): boolean {
        return !this.filters.some(x => !x.hasItem(value, t));
    }

    constructor(private filters: _ISelection<T>[]) {
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

    *enumerate(t: _Transaction): Iterable<T> {
        // sort them so the most restrictive filter is first
        const { best, sorted } = this.plan(t)
        for (const item of best.enumerate(t)) {
            if (!sorted.some(x => x.hasItem(item, t))) {
                continue;
            }
            yield item;
        }
    }
}