import { _IIndex, IValue, _ITable, _IDb, _Transaction, _Explainer, _IndexExplanation, IndexOp, IndexKey, Stats } from '../interfaces-private.ts';
import { ReadOnlyError, NotSupported } from '../interfaces.ts';

interface IndexSubject<T> {
    readonly size: number;
    readonly column: IValue;
    byColumnValue(columnValue: string, t: _Transaction): T[];
}

export class CustomIndex<T> implements _IIndex<T> {
    readonly expressions: IValue<any>[];


    explain(e: _Explainer): _IndexExplanation {
        throw new Error('not implemented');
    }

    constructor(readonly onTable: _ITable<T>, private subject: IndexSubject<T>) {
        this.expressions = [this.subject.column];
    }

    get indexName(): string {
        return null as any;
    }

    entropy(): number {
        return this.subject.size;
    }

    stats(t: _Transaction, key?: IndexKey): Stats | null {
        return null;
    }

    iterateKeys() {
        return null;
    }

    add(raw: any): void {
        throw new ReadOnlyError();
    }

    eqFirst([key]: any, t: _Transaction) {
        for (const its of this.subject.byColumnValue(key, t)) {
            return its;
        }
        return null;
    }


    enumerate(op: IndexOp): Iterable<T> {
        switch (op.type) {
            case 'eq':
                return this.eq(op.key, op.t);
            case 'neq':
                return this.neq(op.key, op.t);
            case 'ge':
                return this.ge(op.key, op.t);
            case 'le':
                return this.le(op.key, op.t);
            case 'gt':
                return this.gt(op.key, op.t);
            case 'lt':
                return this.lt(op.key, op.t);
            case 'outside':
                return this.outside(op.lo, op.hi, op.t);
            case 'inside':
                return this.inside(op.lo, op.hi, op.t);
            case 'nin':
                return this.nin(op.keys, op.t);
            default:
                throw NotSupported.never(op['type']);
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

    *outside(lo: IndexKey, hi: IndexKey, t: _Transaction): Iterable<T> {
        yield* this.lt(lo, t);
        yield* this.gt(hi, t);
    }

    *inside(lo: IndexKey, hi: IndexKey, t: _Transaction): Iterable<T> {
        throw new Error('Not implemented');
    }
}