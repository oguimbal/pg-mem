import { _ISelection, IValue } from '../interfaces-private';
import { FilterBase } from './filter-base';

export class FalseFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    get entropy() {
        return 0;
    }

    hasItem(item: T) {
        return false;
    }

    enumerate(): Iterable<T> {
        return [];
    }
}