import { FilterBase } from './transform-base';
import { _ISelection, _Explainer, _SelectExplanation, _Transaction, Stats } from '../interfaces-private';

export class ArrayFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    entropy() {
        return this.rows.length;
    }

    hasItem(raw: T): boolean {
        return this.rows.includes(raw);
    }

    getIndex() {
        return null;
    }

    constructor(fromTable: _ISelection<T>, public rows: T[]) {
        super(fromTable);
    }

    enumerate(): Iterable<T> {
        return this.rows;
    }

    stats(t: _Transaction): Stats | null {
        return {
            count: this.rows.length,
        };
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'constantSet',
            rawArrayLen: this.rows.length,
        }
    }
}