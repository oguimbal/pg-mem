import { Schema, IMemoryDb, IQuery } from './interfaces';
import { MemoryTable } from './table';
import { _IDb, _ISelection, _ITable } from './interfaces-private';
import { Query } from './query';

export function newDb(): IMemoryDb {
    return new MemoryDb();
}

class MemoryDb implements _IDb {
    private tables = new Map<string, MemoryTable>();

    get query():  IQuery {
        return new Query(this);
    }

    declareTable(table: Schema) {
        const nm = table.name.toLowerCase();
        if (this.tables.has(nm)) {
            throw new Error('Table exists: ' + nm);
        }
        const ret = new MemoryTable(table);
        this.tables.set(nm, ret);
        return ret;
    }

    getTable(name: string): _ITable {
        name = name.toLowerCase();
        return this.tables.get(name);
    }

}