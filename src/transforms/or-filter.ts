import { _ISelection, _IIndex, _ITable, getId, _Transaction } from '../interfaces-private';
import { FilterBase } from './transform-base';


export class OrFilter<T = any> extends FilterBase<T> {

    entropy(t: _Transaction) {
        return this.left.entropy(t) + this.right.entropy(t);
    }

    hasItem(value: T, t: _Transaction): boolean {
        return this.left.hasItem(value, t) && this.right.hasItem(value, t);
    }

    constructor(private left: _ISelection<T>, private right: _ISelection<T>) {
        super(left);
        if (left.columns !== right.columns) { //  istanbul ignore next
            throw new Error('Column set mismatch');
        }
    }

    *enumerate(t: _Transaction): Iterable<T> {
        const yielded = new Set<string>();
        for (const item of this.left.enumerate(t)) {
            yield item;
            yielded.add(getId(item));
        }
        for (const item of this.right.enumerate(t)) {
            if (!yielded.has(getId(item))) {
                yield item;
            }
        }
    }
}