import { Schema, IMemoryDb, IQuery, TableEvent, GlobalEvent, TableNotFound, QueryError } from './interfaces';
import { MemoryTable } from './table';
import { _IDb, _ISelection, _ITable } from './interfaces-private';
import { Query } from './query';
import { initialize } from './transforms/transform-base';
import { buildSelection, buildAlias } from './transforms/selection';
import { buildFilter } from './transforms/build-filter';
import { Adapters } from './adapters';
import { Types } from './datatypes';
import { TablesSchema } from './schema/table-list';
import { ColumnsListSchema } from './schema/columns-list';
import { PgConstraintTable } from './schema/pg-constraints-list';
import { PgClassListTable } from './schema/pg-class-list';
import { PgNamespaceTable } from './schema/pg-namespace-list';
import { PgAttributeTable } from './schema/pg-attribute-list';
import { PgIndexTable } from './schema/pg-index-list';
import { PgTypeTable } from './schema/pg-type-list';

export function newDb(): IMemoryDb {
    initialize({
        buildSelection,
        buildAlias,
        buildFilter,
    });
    return new MemoryDb();
}

class MemoryDb implements _IDb {

    private tables = new Map<string, _ITable>();
    private handlers = new Map<TableEvent | GlobalEvent, Set<(...args: any[]) => any>>();

    readonly adapters = new Adapters(this);
    readonly query = new Query(this);
    private otherSchemas = new Map<string, _IDb>();

    constructor(private defaultSchema?: _IDb, private name?: string) {
        const tbl = this.declareTable({
            name: 'current_schema',
            fields: [
                { id: 'current_schema', type: Types.text() },
            ]
        });
        tbl.insert({ current_schema: 'public' });
        tbl
            .setHidden()
            .setReadonly();

        this.tables.set('pg_constraint', new PgConstraintTable(this));
        this.tables.set('pg_class', new PgClassListTable(this));
        this.tables.set('pg_namespace', new PgNamespaceTable(this));
        this.tables.set('pg_attribute', new PgAttributeTable(this));
        this.tables.set('pg_index', new PgIndexTable(this));
        this.tables.set('pg_type', new PgTypeTable(this));

        if (!name) {
            const schema = this.declareSchema('information_schema');
            // SELECT * FROM "information_schema"."tables" WHERE ("table_schema" = 'public' AND "table_name" = 'user')
            schema.tables.set('tables', new TablesSchema(this))
            schema.tables.set('columns', new ColumnsListSchema(this))
            schema.tables.set('columns', new ColumnsListSchema(this));
        }
    }

    declareSchema(name: string) {
        if (this.name) {
            throw new Error('Only default schema can declare a schema');
        }
        if (this.otherSchemas.has(name)) {
            throw new Error('Schema exists: ' + name);
        }
        const ret = new MemoryDb(this, name);
        this.otherSchemas.set(name, ret);
        return ret;
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


    getTable(name: string, nullIfNotExists?: boolean): _ITable {
        name = name.toLowerCase();
        const got = this.tables.get(name);

        if (!got && !nullIfNotExists) {
            throw new TableNotFound(name);
        }
        return got;
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

    get tablesCount(): number {
        return this.tables.size;
    }

    *listTables(): Iterable<_ITable> {
        for (const t of this.tables.values()) {
            if (!t.hidden) {
                yield t;
            }
        }
    }

    getSchema(db: string): _IDb {
        if (!db || db === 'public') {

            return this.defaultSchema ?? this;
        }
        const got = this.otherSchemas.get(db);
        if(!got) {
            throw new QueryError('schema not found: ' + db);
        }
        return got;
    }

}