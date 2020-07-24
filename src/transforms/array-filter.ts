import { FilterBase } from './transform-base';
import { _ISelection, _Explainer, _SelectExplanation } from '../interfaces-private';

export class ArrayFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    entropy() {
        return this.elts.length;
    }

    hasItem(raw: T): boolean {
        return this.elts.includes(raw);
    }

    getIndex() {
        return null;
    }

    constructor(fromTable: _ISelection<T>, private elts: T[]) {
        super(fromTable);
    }

    enumerate(): Iterable<T> {
        return this.elts;
    }


    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            type: 'constantSet',
            rawArrayLen: this.elts.length,
        }
    }
}