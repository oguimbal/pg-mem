import { _IIndex, IValue, _ITable, _IDb, _Transaction, _Explainer, _IndexExplanation } from '../interfaces-private';
import { ReadOnlyError } from '../interfaces';
import { nullIsh } from '../utils';

interface IndexSubject<T> {
    readonly size: number;
    readonly column: IValue;
    byColumnValue(columnValue: string, t: _Transaction): T[];
}

export class CustomIndex<T> implements _IIndex<T> {
    readonly expressions: IValue<any>[];

    get hash(): string {
        throw new Error('not implemented');
    }
    explain(e: _Explainer): _IndexExplanation {
        throw new Error('not implemented');
    }

    constructor(readonly onTable: _ITable<T>, private subject: IndexSubject<T>) {
        this.expressions = [this.subject.column];
    }

    size(): number {
        return this.subject.size;
    }

    get indexName(): string {
        return null;
    }

    entropy(): number {
        return this.size();
    }

    hasItem(raw: any, t: _Transaction): boolean {
        const got = nullIsh(raw) ? null : this.subject.column.get(raw, t);
        return !nullIsh(got) && this.subject.byColumnValue(got, t).length > 0;
    }

    hasKey([key]: any[], t: _Transaction): boolean {
        return this.subject.byColumnValue(key, t).length > 0;
    }

    add(raw: any): void {
        throw new ReadOnlyError();
    }

    eqFirst([key]: any, t: _Transaction) {
        for (const its of this.subject.byColumnValue(key, t)) {
            return its;
        }
    }
    * eq([rawKey]: any, t: _Transaction): Iterable<any> {
        for (const its of this.subject.byColumnValue(rawKey, t)) {
            yield its;
        }
    }

    * nin(keys: any[][], t: _Transaction) {
        const raws = keys.map(x => x[0]) as any[];
        for (const i of this.onTable.selection.enumerate(t)) {
            const val = this.subject.column.get(i, t);
            if (raws.includes(val)) {
                continue;
            }
            yield i;
        }
    }

    * neq([rawKey]: any, t: _Transaction) {
        for (const i of this.onTable.selection.enumerate(t)) {
            const val = this.subject.column.get(i, t);
            if (val !== rawKey) {
                yield i;
            }
        }
    }
    * gt(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            const val = this.subject.column.get(i, t);
            if (val > rawKey) {
                yield i;
            }
        }
    }
    * lt(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            const val = this.subject.column.get(i, t);
            if (val < rawKey) {
                yield i;
            }
        }
    }
    * ge(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            const val = this.subject.column.get(i, t);
            if (val >= rawKey) {
                yield i;
            }
        }
    }
    * le(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            const val = this.subject.column.get(i, t);
            if (val <= rawKey) {
                yield i;
            }
        }
    }

}