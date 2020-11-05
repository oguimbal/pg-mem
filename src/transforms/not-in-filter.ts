import { _ISelection, IValue, _IIndex, _ITable, getId, _Transaction, _Explainer, _SelectExplanation, IndexKey, Stats } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { DataType, CastError, QueryError } from '../interfaces';
import { nullIsh } from '../utils';

export class NotInFilter<T = any> extends FilterBase<T> {

    private index: _IIndex;
    private keys: IndexKey[];

    entropy(t: _Transaction) {
        return this.onValue.index!.entropy({
            type: 'nin',
            keys: this.keys,
            t,
        });
    }

    hasItem(item: T, t: _Transaction): boolean {
        const val = this.onValue.get(item, t);
        return !nullIsh(val)
            && !this.elts.some(x => this.onValue.type.equals(x, val));
    }

    constructor(private onValue: IValue<T>
        , private elts: any[]) {
        super(onValue.origin!);
        this.index = onValue.index!;
        if (this.index.expressions.length !== 1) {
            throw new Error('Only supports IN with signle expressions index');
        }
        if (!Array.isArray(elts)) {
            throw new QueryError('Cannot iterate element list');
        }
        this.keys = elts.map(x => [x]);
    }


    stats(t: _Transaction): Stats | null {
        const all = this.base.stats(t);
        if (!all) {
            return null;
        }
        const elts = this.elts.map(x => this.index.stats(t, [x]));
        if (elts.some(x => !x)) {
            return null;
        }
        // compute based on 'all'
        for (const i of elts) {
            all.count -= i!.count;
        }
        return all;
    }

    *enumerate(t: _Transaction): Iterable<T> {
        yield* this.index.enumerate({
            type: 'nin',
            keys: this.keys,
            t,
        });
    }


    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'neq',
            entropy: this.entropy(e.transaction),
            on: this.onValue.index!.explain(e),
        };
    }
}