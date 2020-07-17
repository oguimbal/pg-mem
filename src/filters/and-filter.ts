import { _ISelection, _IIndex, _ITable } from '../interfaces-private';
import { FilterBase } from './filter-base';

export function buildAndFilter<T>(left: _ISelection<T>, right: _ISelection<T>) {
    if (left.entropy < right.entropy) {
        return new AndFilter(left, right);
    }
    return new AndFilter(right, left);
}

class AndFilter<T = any> extends FilterBase<T> {

    get index(): _IIndex<T> {
        return null;
    }

    get entropy() {
        return this.lower.entropy;
    }

    hasItem(value: T): boolean {
        return this.lower.hasItem(value) && this.higher.hasItem(value);
    }

    constructor(private lower: _ISelection<T>, private higher: _ISelection<T>) {
        super(lower);
        if (lower.columns !== higher.columns) {
            throw new Error('Column set mismatch');
        }
    }

    *enumerate(): Iterable<T> {
        for (const item of this.lower.enumerate()) {
            if (this.higher.hasItem(item)) {
                yield item;
            }
        }
    }
}