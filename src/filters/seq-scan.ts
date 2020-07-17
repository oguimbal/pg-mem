import { IValue, _ISelection } from '../interfaces-private';
import { DataType } from '../interfaces';
import { FilterBase } from './filter-base';

export class SeqScanFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    get entropy() {
        // boost source entropy (in case an index has the same items count)
        return this.selection.entropy * 1.5;
    }

    hasItem(raw: T): boolean {
        return !!this.getter.get(raw);
    }

    constructor(private selection: _ISelection<T>, private getter: IValue<T>) {
        super(selection);
        this.getter = getter.convert(DataType.bool);
    }

    *enumerate(): Iterable<T> {
        for (const raw of this.selection.enumerate()) {
            const cond = this.getter.get(raw);
            if (cond) {
                yield raw;
            }
        }
    }
}