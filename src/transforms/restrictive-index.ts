import { _IIndex, IValue, IndexExpression, _Transaction, IndexKey, _Explainer, _IndexExplanation, IndexOp, _ISelection } from '../interfaces-private';

export class RestrictiveIndex<T> implements _IIndex<T> {
    constructor(private base: _IIndex<T>, readonly filter: _ISelection<T>) {
        // , readonly match: (raw: T, t: _Transaction) => boolean) {
    }

    private match(raw: T, t: _Transaction) {
        return this.filter.hasItem(raw, t);
    }

    get expressions(): IndexExpression[] {
        return this.base.expressions;
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

    *enumerate(op: IndexOp): Iterable<T> {
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