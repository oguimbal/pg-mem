import { _ISelection, _IIndex, _ITable } from '../interfaces-private';
import { FilterBase } from './filter-base';


export class OrFilter<T = any> extends FilterBase<T> {

    get index(): _IIndex<T> {
        return null;
    }

    get entropy() {
        return this.left.entropy + this.right.entropy;
    }

    hasItem(value: T): boolean {
        return this.left.hasItem(value) && this.right.hasItem(value);
    }

    constructor(private left: _ISelection<T>, private right: _ISelection<T>) {
        super(left);
        if (left.columns !== right.columns) {
            throw new Error('Column set mismatch');
        }
    }

    *enumerate(): Iterable<T> {
        const yielded = new Set<T>();
        for (const item of this.left.enumerate()) {
            yield item;
            yielded.add(item);
        }
        for (const item of this.right.enumerate()) {
            if (!yielded.has(item)) {
                yield item;
            }
        }
    }

    sql(state) {
        state = state ?? { alias: 0 };
        const lower = this.left.sql(state);
        const higher = this.right.sql(state);
        return `(${lower} OR ${higher})`;
    }
}