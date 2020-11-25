import { _IIndex, IValue, _ITable, _IDb, _Transaction, _Explainer, _IndexExplanation, IndexOp, IndexKey, Stats } from '../interfaces-private';
import { ReadOnlyError, NotSupported } from '../interfaces';

export class TableIndex implements _IIndex {
    readonly expressions: IValue<any>[];

    get hash(): string {
        throw new Error('not implemented');
    }
    explain(e: _Explainer): _IndexExplanation {
        throw new Error('not implemented');
    }

    stats(t: _Transaction, key?: IndexKey): Stats | null {
        return null;
    }

    iterateKeys() {
        return null;
    }

    constructor(readonly onTable: _ITable & { itemsByTable(table: string, t: _Transaction): Iterable<any>; ownSymbol: any }, private col: IValue) {
        this.expressions = [col];
    }

    get indexName(): string {
        return 'index_table_name_name';
    }

    entropy(op: IndexOp): number {
        return this.onTable.db.listSchemas()
            .reduce((tot, s) => tot + s.tablesCount(op.t) * 10 * 3, 0);
    }


    add(raw: any): void {
        throw new ReadOnlyError('tables');
    }

    eqFirst([key]: any, t: _Transaction) {
        for (const its of this.onTable.itemsByTable(key, t)) {
            return its;
        }
    }
    *eq([rawKey]: any, t: _Transaction): Iterable<any> {
        for (const its of this.onTable.itemsByTable(rawKey, t)) {
            yield its;
        }
    }

    *nin(keys: any[][], t: _Transaction) {
        const raws = keys.map(x => x[0]) as any[];
        for (const i of this.onTable.selection.enumerate(t)) {
            if (raws.includes(i.table_name)) {
                continue;
            }
            yield i;
        }
    }

    *neq([rawKey]: any, t: _Transaction) {
        for (const i of this.onTable.selection.enumerate(t)) {
            if (i.table_name !== rawKey) {
                yield i;
            }
        }
    }
    *gt(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            if (i.table_name > rawKey) {
                yield i;
            }
        }
    }
    *lt(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            if (i.table_name < rawKey) {
                yield i;
            }
        }
    }
    *ge(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            if (i.table_name >= rawKey) {
                yield i;
            }
        }
    }
    *le(rawKey: any, t: _Transaction): Iterable<any> {
        for (const i of this.onTable.selection.enumerate(t)) {
            if (i.table_name <= rawKey) {
                yield i;
            }
        }
    }


    enumerate(op: IndexOp): Iterable<any> {
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

    *outside(lo: IndexKey, hi: IndexKey, t: _Transaction): Iterable<any> {
        yield* this.lt(lo, t);
        yield* this.gt(hi, t);
    }

    *inside(lo: IndexKey, hi: IndexKey, t: _Transaction): Iterable<any> {
        throw new Error('Not implemented');
    }
}