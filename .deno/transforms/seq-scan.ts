import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats, Row } from '../interfaces-private.ts';
import { FilterBase } from './transform-base.ts';
import { Types } from '../datatypes/index.ts';

export class SeqScanFilter extends FilterBase {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        // boost source entropy (in case an index has the same items count)
        return this.selection.entropy(t) * 1.5;
    }

    hasItem(raw: Row, t: _Transaction): boolean {
        return !!this.getter.get(raw, t);
    }

    constructor(private selection: _ISelection, private getter: IValue) {
        super(selection);
        this.getter = getter.cast(Types.bool);
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
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
            _: 'seqFilter',
            filtered: this.selection.explain(e),
        };
    }
}