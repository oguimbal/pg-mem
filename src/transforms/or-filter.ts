import { _ISelection, _IIndex, _ITable, getId, _Transaction, _Explainer, _SelectExplanation, Stats, Row } from '../interfaces-private';
import { FilterBase } from './transform-base';


export class OrFilter extends FilterBase {

    entropy(t: _Transaction) {
        return this.left.entropy(t) + this.right.entropy(t);
    }

    hasItem(value: Row, t: _Transaction): boolean {
        return this.left.hasItem(value, t) || this.right.hasItem(value, t);
    }

    constructor(private left: _ISelection, private right: _ISelection) {
        super(left);
        if (left.columns !== right.columns) { //  istanbul ignore next
            throw new Error('Column set mismatch');
        }
    }

    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        const yielded = new Set<string>();
        for (const item of this.left.enumerate(t)) {
            yield item;
            yielded.add(getId(item));
        }
        for (const item of this.right.enumerate(t)) {
            const id = getId(item);
            if (!yielded.has(id)) {
                yield item;
            }
        }
    }



    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'union',
            union: [
                this.left.explain(e),
                this.right.explain(e),
            ],
        };
    }
}