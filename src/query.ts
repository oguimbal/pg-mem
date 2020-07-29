import { ISchema, QueryError, DataType, IType, NotSupported, TableNotFound, Schema, QueryResult, SchemaField } from './interfaces';
import { _IDb, _ISelection, CreateIndexColDef, _ISchema, _Transaction, _ITable, _SelectExplanation, _Explainer } from './interfaces-private';
import { watchUse } from './utils';
import { buildValue } from './predicate';
import { Types, fromNative } from './datatypes';
import { JoinSelection } from './transforms/join';
import { Statement, CreateTableStatement, SelectStatement, InsertStatement, CreateIndexStatement, UpdateStatement, AlterTableStatement, DeleteStatement } from './parser/syntax/ast';
import { parse } from './parser/parser';
import { MemoryTable } from './table';
import { buildSelection } from './transforms/selection';
import { ArrayFilter } from './transforms/array-filter';
import { PgConstraintTable, PgClassListTable, PgNamespaceTable, PgAttributeTable, PgIndexTable, PgTypeTable, TablesSchema, ColumnsListSchema } from './schema';

type QR = QueryResult & { ignored?: boolean };
export class Query implements _ISchema, ISchema {

    private dualTable = new MemoryTable(this, this.db.data, { fields: [], name: 'dual' });
    private tables = new Map<string, _ITable>();
    private lastSelect: _ISelection<any>;


    constructor(readonly name: string, readonly db: _IDb) {
        this.dualTable.insert(this.db.data, {});
    }

    pgSchema() {

        this.tables.set('pg_constraint', new PgConstraintTable(this))
        this.tables.set('pg_class', new PgClassListTable(this))
        this.tables.set('pg_namespace', new PgNamespaceTable(this))
        this.tables.set('pg_attribute', new PgAttributeTable(this))
        this.tables.set('pg_index', new PgIndexTable(this))
        this.tables.set('pg_type', new PgTypeTable(this));


        const tbl = this.declareTable({
            name: 'current_schema',
            fields: [
                { name: 'current_schema', type: Types.text() },
            ]
        }, true);
        tbl.insert(this.db.data, { current_schema: this.name });
        tbl.setHidden().setReadonly();
        return this;
    }

    informationSchma() {
        // SELECT * FROM "information_schema"."tables" WHERE ("table_schema" = 'public' AND "table_name" = 'user')
        this.tables.set('tables', new TablesSchema(this));
        this.tables.set('columns', new ColumnsListSchema(this));
    }

    none(query: string): void {
        this.query(query);
    }

    one(query: string): any {
        const [result] = this.many(query);
        return result;
    }

    many(query: string): any[] {
        return this.query(query).rows;
    }

    query(query: string): QueryResult {
        query = query + ';';
        console.log(query);
        console.log('\n');

        let parsed = parse(query);
        if (!Array.isArray(parsed)) {
            parsed = [parsed];
        }
        let last: QR
        let t = this.db.data.fork();
        for (const _p of parsed) {
            if (!_p) {
                continue;
            }
            const p = watchUse(_p);
            switch (p.type) {
                case 'start transaction':
                    t = t.fork();
                    continue;
                case 'commit':
                    t = t.commit();
                    if (!t.isChild) {
                        t = t.fork(); // recreate an implicit transaction
                    }
                    continue;
                case 'rollback':
                    t = t.rollback();
                    continue;
                case 'insert':
                    last = this.executeInsert(t, p);
                    break;
                case 'update':
                    last = this.executeUpdate(t, p);
                    break;
                case 'select':
                    last = this.executeSelect(t, p);
                    break;
                case 'delete':
                    last = this.executeDelete(t, p);
                    break;
                case 'create table':
                    t = t.fullCommit();
                    last = this.executeCreateTable(t, p);
                    t = t.fork();
                    break;
                case 'create index':
                    t = t.fullCommit();
                    last = this.executeCreateIndex(t, p);
                    t = t.fork();
                    break;
                case 'alter table':
                    t = t.fullCommit();
                    last = this.executeAlterRequest(t, p);
                    t = t.fork();
                    break;
                default:
                    throw NotSupported.never(p, 'statement type');
            }
            if (!last.ignored) {
                p.check?.();
            }
        }
        // implicit final commit
        t.fullCommit();
        return last;
    }

    executeAlterRequest(t: _Transaction, p: AlterTableStatement): QR {
        const table = this.db.getSchema(p.table.db)
            .getTable(p.table.table, p.ifExists);
        const nop: QR = {
            command: 'ALTER',
            fields: [],
            rowCount: 0,
            rows: [],
        };
        function ignore() {
            nop.ignored = true;
            return nop;
        }
        if (!table) {
            return nop;
        }
        const change = p.change;
        switch (change.type) {
            case 'rename':
                table.rename(change.to);
                return nop;
            case 'add column': {
                const col = table.selection.getColumn(change.column.name, true);
                if (col) {
                    if (change.ifNotExists) {
                        return ignore();
                    } else {
                        throw new QueryError('Column already exists: ' + col.sql);
                    }
                }
                table.addColumn(change.column, t);
                return nop;
            }
            case 'drop column':
                const col = table.getColumnRef(change.column, change.ifExists);
                if (!col) {
                    return ignore();
                }
                col.drop(t);
                return nop;
            case 'rename column':
                table.getColumnRef(change.column)
                    .rename(change.to, t);
                return nop;
            case 'alter column':
                table.getColumnRef(change.column)
                    .alter(change.alter, t);
                return nop;
            case 'rename constraint':
                throw new NotSupported('rename constraint');
            case 'add constraint':
                table.addConstraint(change.constraint, t);
                return ignore(); // todo
            default:
                throw NotSupported.never(change, 'alter request');

        }
    }

    executeCreateIndex(t: _Transaction, p: CreateIndexStatement): QueryResult {
        const indexName = p.indexName;
        const onTable = this.getTable(p.table);
        const columns = p.expressions
            .map<CreateIndexColDef>(x => {
                return {
                    value: buildValue(onTable.selection, x.expression),
                    nullsLast: x.nulls === 'last', // nulls are first by default
                    desc: x.order === 'desc',
                }
            });
        onTable
            .createIndex(t, {
                columns,
                indexName,
            });
        return {
            command: 'CREATE',
            fields: [],
            rowCount: 0,
            rows: [],
        };
    }

    executeCreateTable(t: _Transaction, p: CreateTableStatement): QueryResult {
        // get creation parameters
        const table = p.name;
        if (this.getTable(table, true)) {
            throw new QueryError('Table exists: ' + table);
        }

        // perform creation
        this.declareTable({
            name: table,
            constraints: p.constraints,
            fields: p.columns
                .map<SchemaField>(f => {
                    return {
                        ...f,
                        type: fromNative(f.dataType),
                        serial: f.dataType.type === 'serial',
                    }
                })
        });
        return {
            command: 'CREATE',
            fields: [],
            rowCount: 0,
            rows: [],
        };
    }

    explainLastSelect(): _SelectExplanation {
        return this.lastSelect?.explain(new Explainer(this.db.data));
    }
    explainSelect(sql: string): _SelectExplanation {
        let parsed = parse(sql);
        if (Array.isArray(parsed)) {
            throw new Error('Expecting a single statement');
        }
        if (parsed.type !== 'select') {
            throw new Error('Expecting a select statement');
        }
        return this.buildSelect(parsed)
            .explain(new Explainer(this.db.data))
    }

    executeDelete(t: _Transaction, p: DeleteStatement): QueryResult {
        const table = this.db.getSchema(p.from.db)
            .getTable(p.from.table);
        const toDelete = table
            .selection
            .filter(p.where);
        const rows = [];
        for (const item of toDelete.enumerate(t)) {
            table.delete(t, item);
            rows.push(item);
        }
        const returning = p.returning && buildSelection(new ArrayFilter(table.selection, rows), p.returning);
        return {
            rows: !returning ? [] : [...returning.enumerate(t)],
            rowCount: rows.length,
            command: 'DELETE',
            fields: [],
        }
    }

    executeSelect(t: _Transaction, p: SelectStatement): QueryResult {
        const subj = this.buildSelect(p);
        this.lastSelect = subj;
        const rows = [...subj.enumerate(t)];
        return {
            rows,
            rowCount: rows.length,
            command: 'SELECT',
            fields: [],
        };
    }

    buildSelect(p: SelectStatement): _ISelection {
        if (p.type !== 'select') {
            throw new NotSupported(p.type);
        }
        let sel: _ISelection;
        const aliases = new Set<string>();
        for (const from of p.from ?? []) {
            const alias = from.type === 'table'
                ? from.alias ?? from.table
                : from.alias;
            if (!alias) {
                throw new Error('No alias provided');
            }
            if (aliases.has(alias)) {
                throw new Error(`Table name "${alias}" specified more than once`)
            }
            // find what to select
            let newT = from.type === 'statement'
                ? this.buildSelect(from.statement)
                : this.db.getSchema(from.db)
                    .getTable(from.table)
                    .selection;

            // set its alias
            newT = newT.setAlias(alias);

            if (!sel) {
                // first table to be selected
                sel = newT;
                continue;
            }


            switch (from.join?.type) {
                case 'INNER JOIN':
                    sel = new JoinSelection(this, sel, newT, from.join.on, true);
                    break;
                case 'LEFT JOIN':
                    sel = new JoinSelection(this, sel, newT, from.join.on, false);
                    break;
                case 'RIGHT JOIN':
                    sel = new JoinSelection(this, newT, sel, from.join.on, false);
                    break;
                default:
                    throw new NotSupported('Joint type not supported ' + (from.join?.type ?? '<no join specified>'));
            }
        }

        // filter & select
        sel = sel ?? this.dualTable.selection;
        sel = sel.filter(p.where);

        if (p.groupBy) {
            sel = sel.groupBy(p.groupBy, p.columns);
        } else {
            sel = sel.select(p.columns);
        }
        if (p.limit) {
            sel = sel.limit(p.limit);
        }
        return sel;
    }

    executeUpdate(t: _Transaction, p: UpdateStatement): QueryResult {
        const into = this.db
            .getSchema(p.table.db)
            .getTable(p.table.table);

        const items = into
            .selection
            .filter(p.where);

        const sets = p.sets.map(x => ({
            ...x,
            getter: x.value !== 'default' && buildValue(items, x.value),
        }));
        const ret = [];
        let rowCount = 0;
        const returning = p.returning && buildSelection(new ArrayFilter(items, ret), p.returning);
        for (const i of items.enumerate(t)) {
            rowCount++;
            for (const s of sets) {
                if (s.value === 'default') {
                    i[s.column] = null;
                } else {
                    i[s.column] = s.getter.get(i, t);
                }
            }
            ret.push(into.update(t, i));
        }

        const rows = returning
            ? [...returning.enumerate(t)]
            : [];

        return {
            rowCount,
            rows,
            command: 'UPDATE',
            fields: [],
        }
    }

    executeInsert(t: _Transaction, p: InsertStatement): QueryResult {
        if (p.type !== 'insert') {
            throw new NotSupported();
        }

        // get table to insert into
        const table = this.db
            .getSchema(p.into.db)
            .getTable(p.into.table);

        // get columns to insert into
        const columns: string[] = p.columns ?? table.selection.columns.map(x => x.id);

        const ret = [];
        const returning = p.returning && buildSelection(new ArrayFilter(table.selection, ret), p.returning);

        // get values to insert
        let rowCount = 0;
        if (p.values) {
            const values = p.values;
            for (const val of values) {
                rowCount++;
                if (val.length !== columns.length) {
                    throw new QueryError('Insert columns / values count mismatch');
                }
                const toInsert = {};
                for (let i = 0; i < val.length; i++) {
                    const v = val[i];
                    const col = table.selection.getColumn(columns[i]);
                    if (v === 'default') {
                        continue;
                    }
                    const notConv = buildValue(null, v);
                    const converted = notConv.convert(col.type);
                    if (!converted.isConstant) {
                        throw new QueryError('Cannot insert non constant expression');
                    }
                    toInsert[columns[i]] = converted.get();
                }
                ret.push(table.insert(t, toInsert));
            }
        } else if (p.select) {
            const selection = this.executeSelect(t, p.select);
            throw new Error('todo: array-mode iteration');
        } else {
            throw new QueryError('Nothing to insert');
        }

        const rows = returning
            ? [...returning.enumerate(t)]
            : [];

        return {
            rowCount,
            rows,
            command: 'INSERT',
            fields: [],
        }
    }

    getTable(name: string, nullIfNotExists?: boolean): _ITable {
        name = name.toLowerCase();
        const got = this.tables.get(name);
        if (!got && !nullIfNotExists) {
            throw new TableNotFound(name);
        }
        return got;
    }


    declareTable(table: Schema, noSchemaChange?: boolean) {
        const nm = table.name.toLowerCase();
        if (this.tables.has(nm)) {
            throw new Error('Table exists: ' + nm);
        }
        const trans = this.db.data.fork();
        const ret = new MemoryTable(this, trans, table);
        trans.commit();
        this.tables.set(nm, ret);
        if (!noSchemaChange) {
            this.db.onSchemaChange();
        }
        return ret;
    }

    _doRenTab(table: string, to: string) {
        const t = this.getTable(table);
        const nm = to.toLowerCase();
        if (this.tables.has(nm)) {
            throw new Error('Table exists: ' + nm);
        }
        const onm = table.toLowerCase();
        this.tables.delete(onm);
        this.tables.set(nm, t);
    }

    tablesCount(t: _Transaction): number {
        return this.tables.size;
    }


    *listTables(): Iterable<_ITable> {
        for (const t of this.tables.values()) {
            if (!t.hidden) {
                yield t;
            }
        }
    }
}

class Explainer implements _Explainer {
    private sels = new Map<_ISelection, number>();
    constructor(readonly transaction: _Transaction) {
    }

    idFor(sel: _ISelection<any>): string | number {
        if (sel.debugId) {
            return sel.debugId;
        }
        if (this.sels.has(sel)) {
            return this.sels.get(sel);
        }
        const id = this.sels.size + 1;
        this.sels.set(sel, id);
        return id;
    }

}