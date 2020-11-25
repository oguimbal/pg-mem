import { ISchema, QueryError, DataType, IType, NotSupported, TableNotFound, Schema, QueryResult, SchemaField, nil, FunctionDefinition } from './interfaces';
import { _IDb, _ISelection, CreateIndexColDef, _ISchema, _Transaction, _ITable, _SelectExplanation, _Explainer, IValue, _IIndex, OnConflictHandler, _FunctionDefinition, _IType } from './interfaces-private';
import { watchUse } from './utils';
import { buildValue } from './predicate';
import { Types, fromNative, makeType } from './datatypes';
import { JoinSelection } from './transforms/join';
import { Statement, CreateTableStatement, SelectStatement, InsertStatement, CreateIndexStatement, UpdateStatement, AlterTableStatement, DeleteStatement, LOCATION, StatementLocation, SetStatement, CreateExtensionStatement } from 'pgsql-ast-parser';
import { MemoryTable } from './table';
import { buildSelection } from './transforms/selection';
import { ArrayFilter } from './transforms/array-filter';
import { parseSql } from './parse-cache';

export class Query implements _ISchema, ISchema {

    private dualTable = new MemoryTable(this, this.db.data, { fields: [], name: 'dual' });
    private tables = new Map<string, _ITable>();
    private lastSelect?: _ISelection<any>;
    private fns = new Map<string, _FunctionDefinition[]>();

    constructor(readonly name: string, readonly db: _IDb) {
        this.dualTable.insert(this.db.data, {});
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


    query(text: string): QueryResult {
        let last: QueryResult | undefined;
        for (const r of this.queries(text)) {
            last = r;
        }
        return last ?? {
            command: text,
            fields: [],
            location: {},
            rowCount: 0,
            rows: [],
        };
    }

    private parse(query: string) {
        return parseSql(query);
    }

    *queries(query: string): Iterable<QueryResult> {
        query = query + ';';
        // console.log(query);
        // console.log('\n');
        try {
            let parsed = this.parse(query);
            if (!Array.isArray(parsed)) {
                parsed = [parsed];
            }
            let t = this.db.data.fork();
            for (const _p of parsed) {
                if (!_p) {
                    continue;
                }

                // query execution
                let last: QueryResult | undefined = undefined;
                try {
                    const p = watchUse(_p);
                    p[LOCATION] = _p[LOCATION];
                    switch (p.type) {
                        case 'start transaction':
                            t = t.fork();
                            break;
                        case 'commit':
                            t = t.commit();
                            if (!t.isChild) {
                                t = t.fork(); // recreate an implicit transaction
                            }
                            break;
                        case 'rollback':
                            t = t.rollback();
                            break;
                        case 'insert':
                            last = this.executeInsert(t, p);
                            break;
                        case 'update':
                            last = this.executeUpdate(t, p);
                            break;
                        case 'select':
                            const subj = this.buildSelect(p);
                            this.lastSelect = subj;
                            const rows = [...subj.enumerate(t)];
                            last = {
                                rows,
                                rowCount: rows.length,
                                command: 'SELECT',
                                fields: [],
                                location: this.locOf(p),
                            };
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
                        case 'create extension':
                            this.executeCreateExtension(p);
                            break;
                        default:
                            throw NotSupported.never(p, 'statement type');
                    }
                    last = last ?? {
                        command: p.type.toUpperCase(),
                        rowCount: 0,
                        fields: [],
                        location: this.locOf(p),
                        rows: [],
                    };
                    if (!last.ignored) {
                        p.check?.();
                    }
                    yield last;
                } catch (e) {
                    e.location = this.locOf(_p);
                    throw e;
                }
            }

            // implicit final commit
            t.fullCommit();
            this.db.raiseGlobal('query', query);
        } catch (e) {
            this.db.raiseGlobal('query-failed', query);
            throw e;
        }
    }

    executeCreateExtension(p: CreateExtensionStatement) {
        const ext = this.db.getExtension(p.extension);
        const schema = p.schema
            ? this.db.getSchema(p.schema)
            : this;
        this.db.raiseGlobal('create-extension', p.extension, schema, p.version, p.from);
        ext(schema);
    }

    private locOf(p: Statement): StatementLocation {
        return p[LOCATION] ?? {};
    }

    private _getTable(p: { table: string; db?: string }): _ITable;
    private _getTable(p: { table: string; db?: string }, nullIfNotFound?: boolean): _ITable | null;
    private _getTable(p: { table: string; db?: string }, nullIfNotFound?: boolean) {
        return p.db
            ? this.db.getSchema(p.db).getTable(p.table, nullIfNotFound, true)
            : this.db.getTable(p.table, nullIfNotFound);
    }

    executeAlterRequest(t: _Transaction, p: AlterTableStatement): QueryResult {
        const table = this._getTable(p.table);

        const nop: QueryResult = {
            command: 'ALTER',
            fields: [],
            rowCount: 0,
            rows: [],
            location: this.locOf(p),
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
                table.addConstraint(change.constraint, t, change.constraintName);
                return nop;
            default:
                throw NotSupported.never(change, 'alter request');

        }
    }

    executeCreateIndex(t: _Transaction, p: CreateIndexStatement): QueryResult {
        const indexName = p.indexName;
        const onTable = this._getTable(p);
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
            location: this.locOf(p),
        };
    }

    executeCreateTable(t: _Transaction, p: CreateTableStatement): QueryResult {
        const ret: QueryResult = {
            command: 'CREATE',
            fields: [],
            rowCount: 0,
            rows: [],
            location: this.locOf(p),
        };
        // get creation parameters
        const table = p.name;
        if (this._getTable({ table }, true)) {
            if (p.ifNotExists) {
                ret.ignored = true;
                return ret;
            }
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
        return ret;
    }

    explainLastSelect(): _SelectExplanation | undefined {
        return this.lastSelect?.explain(new Explainer(this.db.data));
    }
    explainSelect(sql: string): _SelectExplanation {
        let parsed = this.parse(sql);
        if (parsed.length !== 1) {
            throw new Error('Expecting a single statement');
        }
        if (parsed[0].type !== 'select') {
            throw new Error('Expecting a select statement');
        }
        return this.buildSelect(parsed[0])
            .explain(new Explainer(this.db.data))
    }

    executeDelete(t: _Transaction, p: DeleteStatement): QueryResult {
        const table = this._getTable(p.from);
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
            location: this.locOf(p),
        }
    }

    buildSelect(p: SelectStatement): _ISelection {
        if (p.type !== 'select') {
            throw new NotSupported(p.type);
        }
        let sel: _ISelection | undefined = undefined;
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
                : this._getTable(from)
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
                    sel = new JoinSelection(this, sel, newT, from.join.on!, true);
                    break;
                case 'LEFT JOIN':
                    sel = new JoinSelection(this, sel, newT, from.join.on!, false);
                    break;
                case 'RIGHT JOIN':
                    sel = new JoinSelection(this, newT, sel, from.join.on!, false);
                    break;
                default:
                    throw new NotSupported('Joint type not supported ' + (from.join?.type ?? '<no join specified>'));
            }
        }

        // filter & select
        sel = sel ?? this.dualTable.selection;
        sel = sel.filter(p.where);

        if (p.groupBy) {
            sel = sel.groupBy(p.groupBy, p.columns!);
            sel = sel.orderBy(p.orderBy);
        } else {
            sel = sel.orderBy(p.orderBy);
            sel = sel.select(p.columns!);
        }
        if (p.limit) {
            sel = sel.limit(p.limit);
        }
        return sel;
    }

    executeUpdate(t: _Transaction, p: UpdateStatement): QueryResult {
        const into = this._getTable(p.table);

        const items = into
            .selection
            .filter(p.where);

        const setter = this.createSetter(t, into, items, p.sets);
        const ret: any[] = [];
        let rowCount = 0;
        const returning = p.returning && buildSelection(new ArrayFilter(items, ret), p.returning);
        for (const i of items.enumerate(t)) {
            rowCount++;
            setter(i, i);
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
            location: this.locOf(p),
        }
    }
    private createSetter(t: _Transaction, setTable: _ITable, setSelection: _ISelection, _sets: SetStatement[]) {

        const sets = _sets.map(x => {
            const col = (setTable as MemoryTable).getColumnRef(x.column);
            return {
                col,
                value: x.value,
                getter: x.value !== 'default'
                    ? buildValue(setSelection, x.value).convert(col.expression.type)
                    : null,
            };
        });

        return (target: any, source: any) => {
            for (const s of sets) {
                if (s.value === 'default') {
                    target[s.col.expression.id!] = s.col.default?.get() ?? null;
                } else {
                    target[s.col.expression.id!] = s.getter?.get(source, t) ?? null;
                }
            }
        }
    }

    executeInsert(t: _Transaction, p: InsertStatement): QueryResult | undefined {
        if (p.type !== 'insert') {
            throw new NotSupported();
        }

        // get table to insert into
        const table = this._getTable(p.into);
        const selection = table
            .selection
            .setAlias(p.into.alias);


        const ret: any[] = [];
        const returning = p.returning && buildSelection(new ArrayFilter(selection, ret), p.returning);


        let values = p.values;

        if (p.select) {
            // const selection = this.executeSelect(t, p.select);
            throw new Error('todo: array-mode iteration');
        }
        if (!values) {
            throw new QueryError('Nothing to insert');
        }
        if (!values.length) {
            return undefined; // nothing to insert
        }

        // get columns to insert into
        const columns: string[] = p.columns
            ?? table.selection.columns
                .map(x => x.id!)
                .slice(0, values[0].length);

        // build 'on conflict' strategy
        let ignoreConflicts: OnConflictHandler | undefined = undefined;
        if (p.onConflict) {
            // find the targeted index
            const on = p.onConflict.on?.map(x => buildValue(table.selection, x));
            let onIndex: _IIndex | nil = null;
            if (on) {
                onIndex = table.getIndex(...on);
                if (!onIndex?.unique) {
                    throw new QueryError(`There is no unique or exclusion constraint matching the ON CONFLICT specification`);
                }
            }

            // check if 'do nothing'
            if (p.onConflict.do === 'do nothing') {
                ignoreConflicts = { ignore: onIndex ?? 'all' };
            } else {
                if (!onIndex) {
                    throw new QueryError(`ON CONFLICT DO UPDATE requires inference specification or constraint name`);
                }
                const subject = new JoinSelection(this
                    , selection
                    // fake data... we're only using this to get the multi table column resolution:
                    , new ArrayFilter(table.selection, []).setAlias('excluded')
                    , { type: 'boolean', value: false }
                    , false
                );
                const setter = this.createSetter(t, table, subject, p.onConflict.do.sets);
                ignoreConflicts = {
                    onIndex,
                    update: (item, excluded) => {
                        const jitem = subject.buildItem(item, excluded);
                        setter(item, jitem);
                    },
                }
            }
        }

        // insert values
        let rowCount = 0;
        for (const val of values) {
            rowCount++;
            if (val.length !== columns.length) {
                throw new QueryError('Insert columns / values count mismatch');
            }
            const toInsert: any = {};
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
            ret.push(table.insert(t, toInsert, ignoreConflicts));
        }

        const rows = returning
            ? [...returning.enumerate(t)]
            : [];

        return {
            rowCount,
            rows,
            command: 'INSERT',
            fields: [],
            location: this.locOf(p),
        }
    }


    getTable(table: string): _ITable;
    getTable(table: string, nullIfNotFound: false, forceOwn?: boolean): _ITable;
    getTable(name: string, nullIfNotFound?: boolean, forceOwn?: boolean): _ITable | null;
    getTable(name: string, nullIfNotFound?: boolean, forceOwn?: boolean): _ITable | null {
        name = name.toLowerCase();
        // check that has parent
        if (!forceOwn) {
            const got = this.db.getTable(name);
            if (got) {
                return got;
            }
        }
        const got = this.tables.get(name);
        if (!got && !nullIfNotFound) {
            throw new TableNotFound(name);
        }
        return got ?? null;
    }


    declareTable(table: Schema, noSchemaChange?: boolean): MemoryTable {
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

    _settable(tname: string, table: _ITable): this {
        this.tables.set(tname.toLowerCase(), table);
        return this;
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

    registerFunction(fn: FunctionDefinition): this {
        const nm = fn.name.toLowerCase().trim();
        let fns = this.fns.get(nm);
        if (!fns) {
            this.fns.set(nm, fns = []);
        }
        fns.push({
            args: fn.args?.map(x => makeType(x)) ?? [],
            argsVariadic: fn.argsVariadic && makeType(fn.argsVariadic),
            returns: makeType(fn.returns),
            impure: !!fn.impure,
            implementation: fn.implementation,
        });
        return this;
    }

    getFunctions(name: string, arrity: number): _FunctionDefinition[] {
        const matches = this.fns.get(name);
        return !matches
            ? []
            : matches.filter(m => m.args.length === arrity
                || m.args.length < arrity && m.argsVariadic);
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
            return this.sels.get(sel)!;
        }
        const id = this.sels.size + 1;
        this.sels.set(sel, id);
        return id;
    }

}