import { _IIndex, IValue, _ITable, _IDb, _Transaction, _Explainer, _IndexExplanation } from '../interfaces-private';
import { ReadOnlyError } from '../interfaces';

export class TableIndex implements _IIndex {
    readonly expressions: IValue<any>[];

    get hash(): string {
        throw new Error('not implemented');
    }
    explain(e: _Explainer): _IndexExplanation {
        throw new Error('not implemented');
    }


    constructor(readonly onTable: _ITable & { itemsByTable(table: string, t: _Transaction): Iterable<any>; ownSymbol: any }, private col: IValue) {
        this.expressions = [col];
    }

    size(t: _Transaction): number {
        return this.onTable.schema.tablesCount(t);
    }
    get indexName(): string {
        return 'index_table_name_name';
    }
    entropy(t: _Transaction): number {
        return this.size(t);
    }

    hasItem(raw: any): boolean {
        return raw?.[this.onTable.ownSymbol];
    }

    hasKey([key]: any[], t: _Transaction): boolean {
        return !!this.onTable.schema.getTable(key, true);
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
}