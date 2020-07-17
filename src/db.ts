import { Schema, IMemoryDb, IQuery, TableEvent, GlobalEvent } from './interfaces';
import { MemoryTable } from './table';
import { _IDb, _ISelection, _ITable } from './interfaces-private';
import { Query } from './query';
import { initialize } from './transforms/transform-base';
import { buildSelection, buildAlias } from './transforms/selection';
import { buildFilter } from './transforms/build-filter';

export function newDb(): IMemoryDb {
    initialize({
        buildSelection,
        buildAlias,
        buildFilter,
    });
    return new MemoryDb();
}

class MemoryDb implements _IDb {

    private tables = new Map<string, MemoryTable>();
    private handlers = new Map<TableEvent | GlobalEvent, Set<(... args: any[]) => any>>();

    get query(): IQuery {
        return new Query(this);
    }

    declareTable(table: Schema) {
        const nm = table.name.toLowerCase();
        if (this.tables.has(nm)) {
            throw new Error('Table exists: ' + nm);
        }
        const ret = new MemoryTable(this, table);
        this.tables.set(nm, ret);
        return ret;
    }

    getTable(name: string): _ITable {
        name = name.toLowerCase();
        return this.tables.get(name);
    }

    on(event: GlobalEvent | TableEvent, handler: (...args: any[]) => any) {
        let lst = this.handlers.get(event);
        if (!lst) {
            this.handlers.set(event, lst = new Set());
        }
        lst.add(handler);
    }

    raiseTable(table: string, event: TableEvent): void {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h(table);
        }
    }

    raiseGlobal(event: GlobalEvent): void {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h();
        }
    }
}