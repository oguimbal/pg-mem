import { _ISelection, _IIndex, _ITable, getId } from '../interfaces-private';
import { FilterBase } from './filter-base';


export class OrFilter<T = any> extends FilterBase<T> {

    get entropy() {
        return this.left.entropy + this.right.entropy;
    }

    hasItem(value: T): boolean {
        return this.left.hasItem(value) && this.right.hasItem(value);
    }

    constructor(private left: _ISelection<T>, private right: _ISelection<T>) {
        super(left);
        if (left.columns !== right.columns) { //  istanbul ignore next
            throw new Error('Column set mismatch');
        }
    }

    *enumerate(): Iterable<T> {
        const yielded = new Set<string>();
        for (const item of this.left.enumerate()) {
            yield item;
            yielded.add(getId(item));
        }
        for (const item of this.right.enumerate()) {
            if (!yielded.has(getId(item))) {
                yield item;
            }
        }
    }
}