import { _IIndex, IValue, _ITable, _IDb } from '../interfaces-private';
import { ReadOnlyError } from '../interfaces';
import { nullIsh } from '../utils';

interface IndexSubject<T> {
    readonly size: number;
    readonly column: IValue;
    byColumnValue(columnValue: string): T[];
}

export class CustomIndex<T> implements _IIndex<T> {
    readonly expressions: IValue<any>[];

    constructor(readonly onTable: _ITable<T>, private subject: IndexSubject<T>) {
        this.expressions = [this.subject.column];
    }

    get size(): number {
        return this.subject.size;
    }

    get indexName(): string {
        return null;
    }

    get entropy(): number {
        return this.size;
    }

    hasItem(raw: any): boolean {
        const got = nullIsh(raw) ? null : this.subject.column.get(raw);
        return !nullIsh(got) && this.subject.byColumnValue(got).length > 0;
    }

    hasKey([key]: any[]): boolean {
        return this.subject.byColumnValue(key).length > 0;
    }

    add(raw: any): void {
        throw new ReadOnlyError();
    }

    eqFirst([key]: any) {
        for (const its of this.subject.byColumnValue(key)) {
            return its;
        }
    }
    *eq([rawKey]: any): Iterable<any> {
        for (const its of this.subject.byColumnValue(rawKey)) {
            yield its;
        }
    }

    *nin(keys: any[][]) {
        const raws = keys.map(x => x[0]) as any[];
        for (const i of this.onTable.enumerate()) {
            const val = this.subject.column.get(i);
            if (raws.includes(val)) {
                continue;
            }
            yield i;
        }
    }

    *neq([rawKey]: any) {
        for (const i of this.onTable.enumerate()) {
            const val = this.subject.column.get(i);
            if (val !== rawKey) {
                yield i;
            }
        }
    }
    *gt(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            const val = this.subject.column.get(i);
            if (val > rawKey) {
                yield i;
            }
        }
    }
    *lt(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            const val = this.subject.column.get(i);
            if (val < rawKey) {
                yield i;
            }
        }
    }
    *ge(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            const val = this.subject.column.get(i);
            if (val >= rawKey) {
                yield i;
            }
        }
    }
    *le(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            const val = this.subject.column.get(i);
            if (val <= rawKey) {
                yield i;
            }
        }
    }

}