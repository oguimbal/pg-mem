import { Schema, IMemoryDb, IQuery, TableEvent } from './interfaces';
import { MemoryTable } from './table';
import { _IDb, _ISelection, _ITable } from './interfaces-private';
import { Query } from './query';

export function newDb(): IMemoryDb {
    return new MemoryDb();
}

class MemoryDb implements _IDb {

    private tables = new Map<string, MemoryTable>();
    private handlers = new Map<TableEvent, Set<(table: string) => void>>();

    get query():  IQuery {
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

    on(event: TableEvent, handler: (table: string) => any) {
        let lst = this.handlers.get(event);
        if (!lst) {
            this.handlers.set(event, lst = new Set());
        }
        lst.add(handler);
    }

    raise(table: string, event: TableEvent): void {
        const got = this.handlers.get(event);
        for (const h of got ?? []) {
            h(table);
        }
    }
}