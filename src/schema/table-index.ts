import { _IIndex, IValue, _ITable, _IDb } from '../interfaces-private';
import { ReadOnlyError } from '../interfaces';

export class TableIndex implements _IIndex {
    readonly expressions: IValue<any>[];

    constructor(readonly onTable: _ITable & { itemsByTable(table: string): Iterable<any>; db: _IDb, ownSymbol: any }, private col: IValue) {
        this.expressions = [col];
    }

    get size(): number {
        return this.onTable.db.tablesCount;
    }
    get indexName(): string {
        return 'index_table_name_name';
    }
    get entropy(): number {
        return this.size;
    }

    hasItem(raw: any): boolean {
        return raw?.[this.onTable.ownSymbol];
    }

    hasKey([key]: any[]): boolean {
        return !!this.onTable.db.getTable(key, true);
    }

    add(raw: any): void {
        throw new ReadOnlyError('tables');
    }

    eqFirst([key]: any) {
        for (const its of this.onTable.itemsByTable(key)) {
            return its;
        }
    }
    *eq([rawKey]: any): Iterable<any> {
        for (const its of this.onTable.itemsByTable(rawKey)) {
            yield its;
        }
    }

    *nin(keys: any[][]) {
        const raws = keys.map(x => x[0]) as any[];
        for (const i of this.onTable.enumerate()) {
            if (raws.includes(i.table_name)) {
                continue;
            }
            yield i;
        }
    }

    *neq([rawKey]: any) {
        for (const i of this.onTable.enumerate()) {
            if (i.table_name !== rawKey) {
                yield i;
            }
        }
    }
    *gt(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            if (i.table_name > rawKey) {
                yield i;
            }
        }
    }
    *lt(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            if (i.table_name < rawKey) {
                yield i;
            }
        }
    }
    *ge(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            if (i.table_name >= rawKey) {
                yield i;
            }
        }
    }
    *le(rawKey: any): Iterable<any> {
        for (const i of this.onTable.enumerate()) {
            if (i.table_name <= rawKey) {
                yield i;
            }
        }
    }

}