import { watchUse, ignore, errorMessage, suggestColumnName } from '../utils';
import { GLOBAL_VARS, _ISchema, _Transaction, _FunctionDefinition, _ArgDefDetails, IType, _IType, _ISelection, asTable, _IStatement, asView, asSeq, asIndex, CreateIndexColDef, asSelectable, NotSupported, QueryResult, QueryError, nil } from '../interfaces-private';
import { toSql, Statement, DoStatement, CreateFunctionStatement, ShowStatement, WithStatement, WithStatementBinding, TruncateTableStatement, SelectFromUnion, SelectStatement, ValuesStatement, SelectFromStatement, CreateViewStatement, SelectedColumn, CreateMaterializedViewStatement, AlterSequenceStatement, DropIndexStatement, DropTableStatement, DropSequenceStatement, CreateIndexStatement, AlterTableStatement, NodeLocation, QNameMapped, Name } from 'pgsql-ast-parser';
import { buildValue } from '../expression-builder';
import { Types } from '../datatypes';
import { Deletion } from './records-mutations/deletion';
import { Update } from './records-mutations/update';
import { Insert } from './records-mutations/insert';
import { cleanResults } from './clean-results';
import { MutationDataSourceBase } from './records-mutations/mutation-base';
import { ValuesTable } from '../schema/values-table';
import { JoinSelection } from '../transforms/join';
import { View } from '../view';
import { ExecuteCreateTable } from './create-table';
import { ExecuteCreateSequence } from './create-sequence';

export interface StatementResult {
    result: QueryResult;
    transaction: _Transaction;
}

export class StatementExec implements _IStatement {
    private tempBindings = new Map<string, _ISelection | 'no returning'>();
    lastSelect?: _ISelection<any>;

    constructor(readonly schema: _ISchema, private statement: Statement, private pAsSql: string | nil) {
    }

    private get db() {
        return this.schema.db;
    }

    prepare() {

    }

    executeStatement(t: _Transaction): StatementResult {
        try {
            const _p = this.statement;
            // query execution
            let result: QueryResult | undefined = undefined;
            const { checked: p, check } = this.db.options.noAstCoverageCheck
                ? { checked: _p, check: null }
                : watchUse(_p);
            t.clearTransientData();

            switch (p.type) {
                case 'start transaction':
                case 'begin':
                    ignore(p);
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
                case 'with':
                    result = this.executeWith(t, p);
                    break;
                case 'select':
                case 'delete':
                case 'update':
                case 'insert':
                case 'union':
                case 'union all':
                case 'values':
                case 'with recursive':
                    result = this.executeWithable(t, p);
                    break;
                case 'truncate table':
                    result = this.executeTruncateTable(t, p);
                    break;
                case 'create table':
                    t = t.fullCommit();
                    const tbl = new ExecuteCreateTable(this.schema, p).execute(t);
                    result = {
                        ...this.simple('CREATE', p),
                        ignored: tbl === null,
                    };
                    t = t.fork();
                    break;
                case 'create index':
                    t = t.fullCommit();
                    result = this.executeCreateIndex(t, p);
                    t = t.fork();
                    break;
                case 'alter table':
                    t = t.fullCommit();
                    result = this.executeAlterRequest(t, p);
                    t = t.fork();
                    break;
                case 'create extension':
                    this.schema.executeCreateExtension(p);
                    break;
                case 'create sequence':
                    t = t.fullCommit();
                    const seq = new ExecuteCreateSequence(this.schema, p, false).execute(t);
                    result = {
                        ...this.simple('CREATE', p),
                        ignored: seq === null,
                    };
                    t = t.fork();
                    break;
                case 'alter sequence':
                    t = t.fullCommit();
                    result = this.executeAlterSequence(t, p);
                    t = t.fork();
                    break;
                case 'drop index':
                    t = t.fullCommit();
                    result = this.executeDropIndex(t, p);
                    t = t.fork();
                    break;
                case 'drop table':
                    t = t.fullCommit();
                    result = this.executeDropTable(t, p);
                    t = t.fork();
                    break;
                case 'drop sequence':
                    t = t.fullCommit();
                    result = this.executeDropSequence(t, p);
                    t = t.fork();
                    break;
                case 'show':
                    result = this.executeShow(t, p);
                    break;
                case 'set':
                case 'set timezone':
                    if (p.type === 'set' && p.set.type === 'value') {
                        t.set(GLOBAL_VARS, t.getMap(GLOBAL_VARS)
                            .set(p.variable.name, p.set.value));
                        break;
                    }
                    // todo handle set statements timezone ?
                    // They are just ignored as of today (in order to handle pg_dump exports)
                    ignore(p);
                    break;
                case 'create enum':
                    t = t.fullCommit();
                    (p.name.schema ? this.db.getSchema(p.name.schema) : this.schema)
                        .registerEnum(p.name.name, p.values.map(x => x.value));
                    t = t.fork();
                    break;
                case 'tablespace':
                    throw new NotSupported('"TABLESPACE" statement');
                case 'prepare':
                    throw new NotSupported('"PREPARE" statement');
                case 'create view':
                    t = t.fullCommit();
                    result = this.executeCreateView(t, p);
                    t = t.fork();
                    break;
                case 'create materialized view':
                    t = t.fullCommit();
                    result = this.executeCreateMaterializedView(t, p);
                    t = t.fork();
                    break;
                case 'create schema':
                    t = t.fullCommit();
                    const sch = this.db.getSchema(p.name.name, true);
                    if (!p.ifNotExists && sch) {
                        throw new QueryError('schema already exists! ' + p.name);
                    }
                    if (sch) {
                        ignore(p);
                        break;
                    }
                    this.db.createSchema(p.name.name);
                    t = t.fork();
                    break;
                case 'create function':
                    result = this.createFunction(p);
                    break;
                case 'drop function':
                    this.schema.dropFunction(p);
                    result = this.simple('DROP', p);
                    break;
                case 'do':
                    result = this.do(p);
                    break;
                case 'comment':
                case 'raise':
                    ignore(p);
                    break;
                case 'create composite type':
                    // todo: implement composite types
                    throw new NotSupported('create composite type');
                default:
                    throw NotSupported.never(p, 'statement type');
            }
            result = result ?? this.simple(p.type.toUpperCase(), p);
            if (!result.ignored && check) {
                const ret = check();
                if (ret) {
                    throw new NotSupported(ret);
                }
            }
            return { result, transaction: t };
        } catch (e) {

            if (!this.db.options.noErrorDiagnostic && (e instanceof Error) || e instanceof NotSupported) {

                // compute SQL
                const msgs = [e.message];


                if (e instanceof QueryError) {
                    msgs.push(`🐜 This seems to be an execution error, which means that your request syntax seems okay,
but the resulting statement cannot be executed → Probably not a pg-mem error.`);
                } else if (e instanceof NotSupported) {
                    msgs.push(`👉 pg-mem is work-in-progress, and it would seem that you've hit one of its limits.`);
                } else {
                    msgs.push('💥 This is a nasty error, which was unexpected by pg-mem. Also known "a bug" 😁 Please file an issue !')
                }

                if (!this.db.options.noErrorDiagnostic) {
                    if (this.pAsSql) {
                        msgs.push(`*️⃣ Failed SQL statement: ${this.pAsSql}`);
                    } else {
                        try {
                            msgs.push(`*️⃣ Reconsituted failed SQL statement: ${toSql.statement(this.statement)}`);
                        } catch (f) {
                            msgs.push(`*️⃣ <Failed to reconsitute SQL - ${errorMessage(f)}>`);
                        }
                    }
                }
                msgs.push('👉 You can file an issue at https://github.com/oguimbal/pg-mem along with a way to reproduce this error (if you can), and  the stacktrace:')
                e.message = msgs.join('\n\n') + '\n\n';
            }
            if (e && typeof e === 'object') {
                (e as any).location = this.locOf(this.statement);
            }
            throw e;
        }
    }




    getSelectable(name: QNameMapped): _ISelection<any> {
        const temp = !name.schema
            && this.tempBindings.get(name.name);
        if (temp === 'no returning') {
            throw new QueryError(`WITH query "${name.name}" does not have a RETURNING clause`);
        }
        let ret = temp || asSelectable(this.schema.getObject(name)).selection;

        ret = this.mapColumns(name.name, ret, name.columnNames, false);

        if (name.alias) {
            ret = ret.setAlias(name.alias);
        }
        return ret;
    }

    private mapColumns(tableName: string, sel: _ISelection, columnNames: Name[] | nil, appendNonMapped: boolean) {
        if (!columnNames?.length) {
            return sel;
        }
        if (columnNames.length > sel.columns.length) {
            throw new QueryError(`table "${tableName}" has ${sel.columns.length} columns available but ${columnNames.length} columns specified`, '42P10')
        }

        const mapped = new Set<string>(columnNames.map(x => x.name));
        const cols = sel.columns.map<SelectedColumn>((col, i) => ({
            expr: {
                type: 'ref',
                name: col.id!,
            },
            // when realiasing table columns, columns which have not been mapped
            //  must not be removed
            // see ut "can map column names"
            alias: columnNames[i]
                ?? {
                name: mapped.has(sel.columns[i].id!)
                    ? `${sel.columns[i].id!}1`
                    : sel.columns[i].id!,
            },
        }));

        return sel.select(
            cols
        )
    }

    private do(st: DoStatement) {
        const lang = this.db.getLanguage(st.language?.name ?? 'plpgsql');
        const compiled = lang({
            args: [],
            code: st.code,
            schema: this.schema,
        });
        // TODO ACCESS OUTER TRANSACTION WITHIN THIS CALL
        compiled();
        return this.simple('DO', st);
    }


    private createFunction(fn: CreateFunctionStatement) {
        if (!fn.language) {
            throw new QueryError('Unspecified function language');
        }

        const lang = this.db.getLanguage(fn.language.name);

        // determine arg types
        const args = fn.arguments.map<_ArgDefDetails>(a => ({
            name: a.name?.name,
            type: this.schema.getType(a.type),
            default: a.default && buildValue(this.schema.dualTable.selection, a.default),
            mode: a.mode,
        }));

        // determine return type
        let returns: IType | null = null;
        if (fn.returns) {
            switch (fn.returns.kind) {
                case 'table':
                    // Todo: we're losing the typing here :(
                    returns = Types.record.asArray();
                    ignore(fn.returns.columns);
                    break;
                case 'array':
                case null:
                case undefined:
                    returns = this.schema.getType(fn.returns);
                    break;
                default:
                    throw NotSupported.never(fn.returns);
            }
        }

        let argsVariadic: _IType | nil;
        const variad = args.filter(x => x.mode === 'variadic');
        if (variad.length > 1) {
            throw new QueryError(`Expected only one "VARIADIC" argument`);
        } else if (variad.length) {
            argsVariadic = variad[0].type;
        }

        // compile & register the associated function
        const compiled = lang({
            args,
            code: fn.code,
            returns,
            functioName: fn.name.name,
            schema: this.schema,
        });
        this.schema.registerFunction({
            name: fn.name.name,
            returns,
            implementation: compiled,
            args: args.filter(x => x.mode !== 'variadic'),
            argsVariadic,
            impure: fn.purity !== 'immutable',
            allowNullArguments: fn.onNullInput === 'call',
        }, fn.orReplace ?? false);
        return this.simple('CREATE', fn);
    }

    private executeShow(t: _Transaction, p: ShowStatement): QueryResult {
        const got = t.getMap(GLOBAL_VARS);
        if (!got.has(p.variable.name)) {
            throw new QueryError(`unrecognized configuration parameter "${p.variable.name}"`);
        }
        return {
            rows: [{ [p.variable.name]: got.get(p.variable.name) }],
            rowCount: 1,
            command: 'SHOW',
            fields: [],
            location: this.locOf(p),
        };
    }

    private executeWith(t: _Transaction, p: WithStatement): QueryResult {

        try {
            // ugly hack to ensure that the insert/select behaviour of postgres is OK
            // see unit test "only inserts once with statement is executed" for an example.
            const selTrans = p.in.type === 'select' || p.in.type === 'union' ? t.fork() : t;

            // declare temp bindings
            for (const { alias, statement } of p.bind) {
                const prepared = this.prepareWithable(statement);
                if (this.tempBindings.has(alias.name)) {
                    throw new QueryError(` WITH query name "${alias.name}" specified more than once`);
                }
                this.tempBindings.set(alias.name, typeof prepared === 'number' ? 'no returning' : prepared);
            }
            // execute statement
            return this.executeWithable(selTrans, p.in);
        } finally {
            // remove temp bindings
            for (const { alias } of p.bind) {
                this.tempBindings.delete(alias.name);
            }
        }
    }

    private buildWith(p: WithStatement): _ISelection {
        try {
            // declare temp bindings
            for (const { alias, statement } of p.bind) {
                const prepared = this.buildSelect(this.checkReadonlyWithable(statement))
                    .setAlias(alias.name);
                if (this.tempBindings.has(alias.name)) {
                    throw new QueryError(` WITH query name "${alias.name}" specified more than once`);
                }
                this.tempBindings.set(alias.name, typeof prepared === 'number' ? 'no returning' : prepared);
            }
            return this.buildSelect(this.checkReadonlyWithable(p.in));
        } finally {
            // remove temp bindings
            for (const { alias } of p.bind) {
                this.tempBindings.delete(alias.name);
            }
        }
    }

    private checkReadonlyWithable(st: WithStatementBinding) {
        switch (st.type) {
            case 'delete':
            case 'insert':
            case 'update':
                throw new NotSupported(`"WITH" nested statement with query type '${st.type}'`);
        }
        return st;
    }

    private prepareWithable(p: WithStatementBinding): _ISelection {
        switch (p.type) {
            case 'select':
            case 'union':
            case 'union all':
            case 'with':
            case 'with recursive':
            case 'values':
                return this.lastSelect = this.buildSelect(p);
            case 'delete':
                return this.lastSelect = new Deletion(this, p);
            case 'update':
                return this.lastSelect = new Update(this, p);
            case 'insert':
                return this.lastSelect = new Insert(this, p);
            default:
                throw NotSupported.never(p);
        }
    }

    private executeWithable(t: _Transaction, p: WithStatementBinding) {
        let last = this.prepareWithable(p);

        const rows = typeof last === 'number'
            ? []
            : cleanResults([...last.enumerate(t)]);
        return {
            rows,
            rowCount: t.getTransient(MutationDataSourceBase.affectedRows) ?? rows.length,
            command: p.type.toUpperCase(),
            fields: [],
            location: this.locOf(p),
        };
    }

    executeTruncateTable(t: _Transaction, p: TruncateTableStatement): QueryResult {
        if (p.tables.length !== 1) {
            throw new NotSupported('Multiple truncations');
        }
        const table = asTable(this.schema.getObject(p.tables[0]));
        table.truncate(t);
        return this.simple('TRUNCATE', p);
    }

    private buildUnion(p: SelectFromUnion): _ISelection {
        const left = this.buildSelect(p.left);
        const right = this.buildSelect(p.right);
        const ret = left.union(right);
        if (p.type === 'union all') {
            return ret;
        }
        return ret.distinct();
    }

    buildSelect(p: SelectStatement): _ISelection {
        switch (p.type) {
            case 'union':
            case 'union all':
                return this.buildUnion(p);
            case 'with':
                return this.buildWith(p);
            case 'select':
                return this.buildRawSelect(p);
            case 'values':
                return this.buildValues(p);
            case 'with recursive':
                throw new NotSupported('recursirve with statements not implemented by pg-mem');
            default:
                throw NotSupported.never(p);
        }
    }


    buildValues(p: ValuesStatement, acceptDefault?: boolean): _ISelection {
        const ret = new ValuesTable(this.schema, '', p.values, null, acceptDefault);
        return ret.selection;
    }

    private buildRawSelect(p: SelectFromStatement): _ISelection {
        const distinct = !p.distinct || p.distinct === 'all'
            ? null
            : p.distinct;

        // ignore "for update" clause (not useful in non-concurrent environements)
        ignore(p.for);

        // compute data source
        let sel: _ISelection | undefined = undefined;
        for (const from of p.from ?? []) {
            // find what to select
            let newT: _ISelection;
            switch (from.type) {
                case 'table':
                    newT = this.getSelectable(from.name);
                    break;
                case 'statement':
                    newT = this.mapColumns(from.alias
                        , this.buildSelect(from.statement)
                        , from.columnNames
                        , true)
                        .setAlias(from.alias);
                    break;
                case 'call':
                    const fnName = from.alias?.name ?? from.function?.name;
                    newT = new ValuesTable(this.schema, fnName, [[from]], [fnName])
                        .setAlias(from.alias?.name ?? suggestColumnName(from) ?? '');
                    break;
                default:
                    throw NotSupported.never(from);
            }

            // if (!!newT.name && aliases.has(newT.name)) {
            //     throw new Error(`Alias name "${newT.name}" specified more than once`)
            // }

            if (!sel) {
                // first table to be selected
                sel = newT;
                continue;
            }

            switch (from.join?.type) {
                case 'INNER JOIN':
                    sel = new JoinSelection(this, sel, newT, from.join!, true);
                    break;
                case 'LEFT JOIN':
                    sel = new JoinSelection(this, sel, newT, from.join!, false);
                    break;
                case 'RIGHT JOIN':
                    sel = new JoinSelection(this, newT, sel, from.join!, false);
                    break;
                default:
                    throw new NotSupported('Joint type not supported ' + (from.join?.type ?? '<no join specified>'));
            }
        }

        // filter & select
        sel = sel ?? this.schema.dualTable.selection;
        sel = sel.filter(p.where);

        if (p.groupBy) {
            sel = sel.groupBy(p.groupBy, p.columns!);
            sel = sel.orderBy(p.orderBy);

            // when grouping by, distinct is handled after selection
            //  => can distinct on key, or selected
            if (Array.isArray(p.distinct)) {
                sel = sel.distinct(p.distinct);
            }
        } else {
            sel = sel.orderBy(p.orderBy);

            // when not grouping by, distinct is handled before
            // selection => can distinct on non selected values
            if (Array.isArray(p.distinct)) {
                sel = sel.distinct(p.distinct);
            }

            sel = sel.select(p.columns!);
        }

        // handle 'distinct' on result set
        if (distinct === 'distinct') {
            sel = sel.distinct();
        }

        if (p.limit) {
            sel = sel.limit(p.limit);
        }
        return sel;
    }


    executeCreateView(t: _Transaction, p: CreateViewStatement): QueryResult {

        const nop = this.simple('CREATE', p);
        const onSchema = p.name.schema && p.name.schema !== this.schema.name
            ? this.db.getSchema(p.name.schema)
            : this.schema;

        // check existence
        const existing = asView(this.schema.getObject(p.name, { nullIfNotFound: true }));
        if (p.orReplace && existing) {
            existing.drop(t);
        }

        let view = this.buildSelect(p.query);

        // optional column mapping
        if (p.columnNames?.length) {
            if (p.columnNames.length > view.columns.length) {
                throw new QueryError('CREATE VIEW specifies more column names than columns', '42601');
            }
            view = view.select(view.columns.map<string | SelectedColumn>((x, i) => {
                const alias = p.columnNames?.[i]?.name;
                if (!alias) {
                    return x.id!;
                }
                return {
                    expr: { type: 'ref', name: x.id! },
                    alias: { name: alias },
                }
            }));
        }

        // view creation
        new View(onSchema, p.name.name, view)
            .register();

        return nop;

    }

    executeCreateMaterializedView(t: _Transaction, p: CreateMaterializedViewStatement): QueryResult {

        const nop = this.simple('CREATE', p);
        const onSchema = p.name.schema && p.name.schema !== this.schema.name
            ? this.db.getSchema(p.name.schema)
            : this.schema;

        // check existence
        const existing = asView(this.schema.getObject(p.name, { nullIfNotFound: true }));
        if (p.ifNotExists && existing) {
            nop.ignored = true;
            return nop;
        }

        const view = this.buildSelect(p.query);

        // hack: materialized views are implemented as simple views :/  (todo ?)
        new View(onSchema, p.name.name, view)
            .register();

        return nop;
    }


    executeAlterSequence(t: _Transaction, p: AlterSequenceStatement): QueryResult {

        const nop = this.simple('ALTER', p);

        const got = asSeq(this.schema.getObject(p.name, {
            nullIfNotFound: p.ifExists,
        }));

        if (!got) {
            nop.ignored = true;
            return nop;
        }

        got.alter(t, p.change);

        return nop;
    }


    executeDropIndex(t: _Transaction, p: DropIndexStatement): QueryResult {

        const nop = this.simple('DROP', p);


        const got = asIndex(this.schema.getObject(p.name, {
            nullIfNotFound: p.ifExists,
        }));

        ignore(p.concurrently);
        if (!got) {
            nop.ignored = true;
            return nop;
        }

        got.onTable.dropIndex(t, got.name);
        return nop;
    }

    executeDropTable(t: _Transaction, p: DropTableStatement): QueryResult {

        const nop = this.simple('DROP', p);

        const got = asTable(this.schema.getObject(p.name, {
            nullIfNotFound: p.ifExists,
        }));

        if (!got) {
            nop.ignored = true;
            return nop;
        }

        got.drop(t);
        return nop;
    }


    executeDropSequence(t: _Transaction, p: DropSequenceStatement): QueryResult {

        const nop = this.simple('DROP', p);

        const got = asSeq(this.schema.getObject(p.name, {
            nullIfNotFound: p.ifExists,
        }));

        if (!got) {
            nop.ignored = true;
            return nop;
        }

        got.drop(t);
        return nop;
    }


    executeCreateIndex(t: _Transaction, p: CreateIndexStatement): QueryResult {

        // check that index algorithm is supported
        const indexName = p.indexName?.name;
        const onTable = asTable(this.schema.getObject(p.table));
        if (p.using && p.using.name.toLowerCase() !== 'btree') {
            if (this.db.options.noIgnoreUnsupportedIndices) {
                throw new NotSupported('index type: ' + p.using);
            }
            ignore(p);
            return this.simple('CREATE', p);
        }

        // index columns
        const columns = p.expressions
            .map<CreateIndexColDef>(x => {
                return {
                    value: buildValue(onTable.selection, x.expression),
                    nullsLast: x.nulls === 'last', // nulls are first by default
                    desc: x.order === 'desc',
                }
            });

        // compile predicate (if any)
        const predicate = p.where && buildValue(onTable.selection, p.where);

        // create index
        onTable
            .createIndex(t, {
                columns,
                indexName,
                unique: p.unique,
                ifNotExists: p.ifNotExists,
                predicate,
            });
        return this.simple('CREATE', p);
    }

    executeAlterRequest(t: _Transaction, p: AlterTableStatement): QueryResult {
        const table = asTable(this.schema.getObject(p.table));

        const nop = this.simple('ALTER', p);

        function _ignore() {
            nop.ignored = true;
            return nop;
        }
        if (!table) {
            return nop;
        }

        ignore(p.only);
        for (const change of p.changes) {
            switch (change.type) {
                case 'rename':
                    table.rename(change.to.name);
                    break;
                case 'add column': {
                    const col = table.selection.getColumn(change.column.name.name, true);
                    if (col) {
                        if (change.ifNotExists) {
                            return _ignore();
                        } else {
                            throw new QueryError('Column already exists: ' + col.id);
                        }
                    }
                    table.addColumn(change.column, t);
                    break;
                }
                case 'drop column':
                    const col = table.getColumnRef(change.column.name, change.ifExists);
                    if (!col) {
                        return _ignore();
                    }
                    col.drop(t);
                    break;
                case 'drop constraint':
                    const cst = table.getConstraint(change.constraint.name);
                    if (change.ifExists && !cst) {
                        return _ignore();
                    }
                    if (!cst) {
                        throw new QueryError(`constraint "${change.constraint.name}" of relation "${table.name}" does not exist`, '42704')
                    }
                    cst.uninstall(t);
                    break;
                case 'rename column':
                    table.getColumnRef(change.column.name)
                        .rename(change.to.name, t);
                    break;
                case 'alter column':
                    table.getColumnRef(change.column.name)
                        .alter(change.alter, t);
                    break;
                case 'rename constraint':
                    throw new NotSupported('rename constraint');
                case 'add constraint':
                    table.addConstraint(change.constraint, t);
                    break;
                case 'owner':
                    // owner change statements are not supported.
                    // however, in order to support, pg_dump, we're just ignoring them.
                    _ignore();
                    break;
                default:
                    throw NotSupported.never(change, 'alter request');

            }
        }
        return nop;
    }

    private simple(op: string, p: Statement): QueryResult {
        return {
            command: op,
            fields: [],
            rowCount: 0,
            rows: [],
            location: this.locOf(p),
        };
    }

    private locOf(p: Statement): NodeLocation {
        return p._location ?? { start: 0, end: 0 };
    }
}