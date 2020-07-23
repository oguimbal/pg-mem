import { FilterBase } from './transform-base';
import { _ISelection } from '../interfaces-private';

export class ArrayFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    get entropy() {
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
}