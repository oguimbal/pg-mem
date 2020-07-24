import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation } from '../interfaces-private';
import { DataType } from '../interfaces';
import { FilterBase } from './transform-base';

export class SeqScanFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        // boost source entropy (in case an index has the same items count)
        return this.selection.entropy(t) * 1.5;
    }

    hasItem(raw: T, t: _Transaction): boolean {
        return !!this.getter.get(raw, t);
    }

    constructor(private selection: _ISelection<T>, private getter: IValue<T>) {
        super(selection);
        this.getter = getter.convert(DataType.bool);
    }

    *enumerate(t: _Transaction): Iterable<T> {
        for (const raw of this.selection.enumerate(t)) {
            const cond = this.getter.get(raw, t);
            if (cond) {
                yield raw;
            }
        }
    }



    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            type: 'seqFilter',
            filter: this.selection.explain(e),
        };
    }
}