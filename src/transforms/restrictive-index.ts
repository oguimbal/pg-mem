import { _IIndex, IValue, IndexExpression, _Transaction, IndexKey, _Explainer, _IndexExplanation, IndexOp, _ISelection, Stats, Row } from '../interfaces-private';

export class RestrictiveIndex implements _IIndex {
    constructor(private base: _IIndex, readonly filter: _ISelection) {
    }

    private match(raw: Row, t: _Transaction) {
        return this.filter.hasItem(raw, t);
    }

    get expressions(): IndexExpression[] {
        return this.base.expressions;
    }

    stats(t: _Transaction, key?: IndexKey): Stats | null {
        // cannot comput without iterating
        return null;
    }

    iterateKeys() {
        // cannot comput without iterating
        // (we know underlying keys, but we dont know which have items that match our filter)
        return null;
    }

    eqFirst(rawKey: IndexKey, t: _Transaction) {
        for (const i of this.base.enumerate({
            key: rawKey,
            t: t,
            type: 'eq',
        })) {
            if (this.match(i, t)) {
                return i;
            }
        }
        return null;
    }


    entropy(t: IndexOp): number {
        return this.base.entropy(t);
    }

    *enumerate(op: IndexOp): Iterable<Row> {
        for (const i of this.base.enumerate(op)) {
            if (this.match(i, op.t)) {
                yield i;
            }
        }
    }

    explain(e: _Explainer): _IndexExplanation {
        return {
            _: 'indexRestriction',
            lookup: this.base.explain(e),
            for: this.filter.explain(e),
            // criteria: this.restrict.explain(e),
        }
    }
}