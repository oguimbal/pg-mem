import { ISchema, DataType, IType, NotSupported, RelationNotFound, Schema, QueryResult, SchemaField, nil, FunctionDefinition, PermissionDeniedError, TypeNotFound, ArgDefDetails, IEquivalentType, QueryInterceptor, ISubscription, QueryError, typeDefToStr } from './interfaces';
import { _IDb, _ISelection, CreateIndexColDef, _ISchema, _Transaction, _ITable, _SelectExplanation, _Explainer, IValue, _IIndex, OnConflictHandler, _FunctionDefinition, _IType, _IRelation, QueryObjOpts, _ISequence, asSeq, asTable, _INamedIndex, asIndex, RegClass, Reg, TypeQuery, asType, ChangeOpts, GLOBAL_VARS, _ArgDefDetails, BeingCreated, asView, asSelectable } from './interfaces-private';
import { asSingleQName, errorMessage, ignore, isType, Optional, parseRegClass, pushContext, randomString, schemaOf, suggestColumnName, watchUse, nullIsh } from './utils';
import { buildValue } from './expression-builder';
import { ArrayType, Types, typeSynonyms } from './datatypes';
import { JoinSelection } from './transforms/join';
import { Statement, CreateTableStatement, SelectStatement, InsertStatement, CreateIndexStatement, UpdateStatement, AlterTableStatement, DeleteStatement, SetStatement, CreateExtensionStatement, CreateSequenceStatement, AlterSequenceStatement, QName, QNameAliased, astMapper, DropIndexStatement, DropTableStatement, DropSequenceStatement, toSql, TruncateTableStatement, CreateSequenceOptions, DataTypeDef, ArrayDataTypeDef, BasicDataTypeDef, Expr, WithStatement, WithStatementBinding, SelectFromUnion, ShowStatement, CreateViewStatement, CreateMaterializedViewStatement, CreateFunctionStatement, DoStatement, ColumnConstraint, CreateColumnsLikeTableOpt, NodeLocation, SelectedColumn, SelectFromStatement, ValuesStatement, QNameMapped, Name, DropFunctionStatement } from 'pgsql-ast-parser';
import { MemoryTable } from './table';
import { buildSelection } from './transforms/selection';
import { ArrayFilter } from './transforms/array-filter';
import { parseSql } from './parse-cache';
import { Sequence } from './sequence';
import { IMigrate } from './migrate/migrate-interfaces';
import { migrate } from './migrate/migrate';
import { CustomEnumType } from './datatypes/t-custom-enum';
import { regGen } from './datatypes/datatype-base';
import { ValuesTable } from './schema/values-table';
import { cleanResults } from './clean-results';
import { EquivalentType } from './datatypes/t-equivalent';
import { View } from './view';


type WithableResult = number | _ISelection;

export class DbSchema implements _ISchema, ISchema {

    readonly dualTable: _ITable;
    private relsByNameCas = new Map<string, _IRelation>();
    private relsByCls = new Map<number, _IRelation>();
    private relsByTyp = new Map<number, _IRelation>();
    private tempBindings = new Map<string, _ISelection | 'no returning'>();
    private _tables = new Set<_ITable>();

    private lastSelect?: _ISelection<any>;
    private fns = new Map<string, _FunctionDefinition[]>();
    private installedExtensions = new Set<string>();
    private readonly: any;
    private interceptors = new Set<{ readonly intercept: QueryInterceptor }>();

    constructor(readonly name: string, readonly db: _IDb) {
        this.dualTable = new MemoryTable(this, this.db.data, { fields: [], name: 'dual' }).register();
        this.dualTable.insert({});
        this.dualTable.setReadonly();
        this._reg_unregister(this.dualTable);
    }

    setReadonly() {
        this.readonly = true;
        return this;
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
        // intercept ?
        for (const { intercept } of this.interceptors) {
            const ret = intercept(text);
            if (ret) {
                return {
                    command: text,
                    fields: [],
                    location: { start: 0, end: text.length },
                    rowCount: 0,
                    rows: ret,
                };
            }
        }

        // execute.
        let last: QueryResult | undefined;
        for (const r of this.queries(text)) {
            last = r;
        }
        return last ?? {
            command: text,
            fields: [],
            location: { start: 0, end: text.length },
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

                const { transaction, last } = pushContext({
                    transaction: t,
                    schema: this
                }, () => this._execOne(t, _p, parsed.length === 1 ? query : undefined));
                yield last;
                t = transaction;
            }

            // implicit final commit
            t.fullCommit();
            this.db.raiseGlobal('query', query);
        } catch (e) {
            this.db.raiseGlobal('query-failed', query);
            throw e;
        }
    }


    private _execOne(t: _Transaction, _p: Statement, pAsSql?: string) {
        try {
            // query execution
            let last: QueryResult | undefined = undefined;
            const { checked: p, check } = this.db.options.noAstCoverageCheck
                ? { checked: _p, check: null }
                : watchUse(_p);

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
                    last = this.executeWith(t, p);
                    break;
                case 'select':
                case 'delete':
                case 'update':
                case 'insert':
                case 'union':
                case 'union all':
                case 'values':
                case 'with recursive':
                    last = this.executeWithable(t, p);
                    break;
                case 'truncate table':
                    last = this.executeTruncateTable(t, p);
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
                case 'create sequence':
                    t = t.fullCommit();
                    last = this.executeCreateSequence(t, p);
                    t = t.fork();
                    break;
                case 'alter sequence':
                    t = t.fullCommit();
                    last = this.executeAlterSequence(t, p);
                    t = t.fork();
                    break;
                case 'drop index':
                    t = t.fullCommit();
                    last = this.executeDropIndex(t, p);
                    t = t.fork();
                    break;
                case 'drop table':
                    t = t.fullCommit();
                    last = this.executeDropTable(t, p);
                    t = t.fork();
                    break;
                case 'drop sequence':
                    t = t.fullCommit();
                    last = this.executeDropSequence(t, p);
                    t = t.fork();
                    break;
                case 'show':
                    last = this.executeShow(t, p);
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
                    (p.name.schema ? this.db.getSchema(p.name.schema) : this)
                        .registerEnum(p.name.name, p.values.map(x => x.value));
                    t = t.fork();
                    break;
                case 'tablespace':
                    throw new NotSupported('"TABLESPACE" statement');
                case 'prepare':
                    throw new NotSupported('"PREPARE" statement');
                case 'create view':
                    t = t.fullCommit();
                    last = this.executeCreateView(t, p);
                    t = t.fork();
                    break;
                case 'create materialized view':
                    t = t.fullCommit();
                    last = this.executeCreateMaterializedView(t, p);
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
                    last = this.createFunction(p);
                    break;
                case 'drop function':
                    last = this.dropFunction(p);
                    break;
                case 'do':
                    last = this.do(p);
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
            last = last ?? this.simple(p.type.toUpperCase(), p);
            if (!last.ignored && check) {
                const ret = check();
                if (ret) {
                    throw new NotSupported(ret);
                }
            }
            return { last, transaction: t };
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
                    if (pAsSql) {
                        msgs.push(`*️⃣ Failed SQL statement: ${pAsSql}`);
                    } else {
                        try {
                            msgs.push(`*️⃣ Reconsituted failed SQL statement: ${toSql.statement(_p)}`);
                        } catch (f) {
                            msgs.push(`*️⃣ <Failed to reconsitute SQL - ${errorMessage(f)}>`);
                        }
                    }
                }
                msgs.push('👉 You can file an issue at https://github.com/oguimbal/pg-mem along with a way to reproduce this error (if you can), and  the stacktrace:')
                e.message = msgs.join('\n\n') + '\n\n';
            }
            if (e && typeof e === 'object') {
                (e as any).location = this.locOf(_p);
            }
            throw e;
        }
    }

    private do(st: DoStatement) {
        const lang = this.db.getLanguage(st.language?.name ?? 'plpgsql');
        const compiled = lang({
            args: [],
            code: st.code,
        });
        // TODO ACCESS OUTER TRANSACTION WITHIN THIS CALL
        compiled();
        return this.simple('DO', st);
    }

    private dropFunction(fn: DropFunctionStatement): QueryResult {
        if (fn.name.schema && fn.name.schema !== this.name) {
            return (this.db.getSchema(fn.name.schema) as DbSchema).dropFunction(fn);
        }
        let fns = this.fns.get(fn.name.name);

        // === determine which function to delete
        let toRemove: _FunctionDefinition;
        if (fn.arguments) {
            const targetArgs = fn.arguments;
            const match = fns?.filter(x => x.args.length === targetArgs.length
                && !x.args.some((a, i) => a.type !== this.getType(targetArgs[i].type)));
            if (!match?.length) {
                if (fn.ifExists) {
                    return this.simple('DROP', fn);
                }
                throw new QueryError(`function ${fn.name.name}(${targetArgs.map(t => typeDefToStr(t.type)).join(',')}) does not exist`, '42883');
            }
        } else {
            if (!fns?.length) {
                if (fn.ifExists) {
                    return this.simple('DROP', fn);
                }
                throw new QueryError(`could not find a function named "${fn.name.name}"`, '42883');
            }
            if (fns?.length !== 1) {
                throw new QueryError(`function name "${fn.name.name}" is not unique`, '42725');
            }
            toRemove = fns[0];
        }


        fns = fns!.filter(x => x !== toRemove);
        if (!fns.length) {
            this.fns.delete(fn.name.name);
        } else {
            this.fns.set(fn.name.name, fns);
        }
        return this.simple('DROP', fn);
    }

    private createFunction(fn: CreateFunctionStatement) {
        if (!fn.language) {
            throw new QueryError('Unspecified function language');
        }

        const lang = this.db.getLanguage(fn.language.name);

        // determine arg types
        const args = fn.arguments.map<_ArgDefDetails>(a => ({
            name: a.name?.name,
            type: this.getType(a.type),
            default: a.default && buildValue(this.dualTable.selection, a.default),
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
                    returns = this.getType(fn.returns);
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
        });
        this.registerFunction({
            name: fn.name.name,
            returns,
            implementation: compiled,
            args: args.filter(x => x.mode !== 'variadic'),
            argsVariadic,
            impure: fn.purity !== 'immutable',
            allowNullArguments: fn.onNullInput === 'call',
        }, fn.orReplace);
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
                const prepared = this.prepareWithable(t, statement);
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

    private prepareWithable(t: _Transaction, p: WithStatementBinding): WithableResult {
        switch (p.type) {
            case 'select':
            case 'union':
            case 'union all':
            case 'with':
            case 'with recursive':
            case 'values':
                return this.lastSelect = this.buildSelect(p);
            case 'delete':
                return this.executeDelete(t, p);
            case 'update':
                return this.executeUpdate(t, p);
            case 'insert':
                return this.executeInsert(t, p);
            default:
                throw NotSupported.never(p);
        }
    }

    private executeWithable(t: _Transaction, p: WithStatementBinding) {
        let last = this.prepareWithable(t, p);

        const rows = typeof last === 'number'
            ? []
            : cleanResults([...last.enumerate(t)]);
        return {
            rows,
            rowCount: typeof last === 'number' ? last : rows.length,
            command: p.type.toUpperCase(),
            fields: [],
            location: this.locOf(p),
        };
    }


    registerEnum(name: string, values: string[]) {
        new CustomEnumType(this, name, values).install();
    }

    private checkExistence<T>(command: T, name: QName, ifNotExists: boolean | undefined, act: () => T | null | void): T {
        // check if object exists
        const exists = this.getObject(name, {
            skipSearch: true,
            nullIfNotFound: true
        });
        if (exists) {
            if (ifNotExists) {
                return {
                    ...command,
                    ignored: true,
                };
            }
            throw new QueryError(`relation "${name.name}" already exists`);
        }

        // else, perform operation
        return act() || command;
    }

    executeCreateExtension(p: CreateExtensionStatement) {
        const ext = this.db.getExtension(p.extension.name);
        const schema = p.schema
            ? this.db.getSchema(p.schema.name)
            : this;
        this.db.raiseGlobal('create-extension', p.extension, schema, p.version, p.from);
        const ne = p.ifNotExists; // evaluate outside
        if (this.installedExtensions.has(p.extension.name)) {
            if (ne) {
                return;
            }
            throw new QueryError('Extension already created !');
        }

        ext(schema);
        this.installedExtensions.add(p.extension.name);
    }

    executeCreateView(t: _Transaction, p: CreateViewStatement): QueryResult {

        const nop = this.simple('CREATE', p);
        const onSchema = p.name.schema && p.name.schema !== this.name
            ? this.db.getSchema(p.name.schema)
            : this;

        // check existence
        const existing = asView(this.getObject(p.name, { nullIfNotFound: true }));
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
        const onSchema = p.name.schema && p.name.schema !== this.name
            ? this.db.getSchema(p.name.schema)
            : this;

        // check existence
        const existing = asView(this.getObject(p.name, { nullIfNotFound: true }));
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


    private locOf(p: Statement): NodeLocation {
        return p._location ?? { start: 0, end: 0 };
    }


    private simpleTypes: { [key: string]: _IType } = {};
    private sizeableTypes: {
        [key: string]: {
            ctor: (...sz: number[]) => _IType;
            regs: Map<number | string | undefined, _IType>;
        };
    } = {};


    parseType(native: string): _IType {
        if (/\[\]$/.test(native)) {
            const inner = this.parseType(native.substr(0, native.length - 2));
            return inner.asArray();
        }
        return this.getType({ name: native });
    }


    getOwnType(t: DataTypeDef): _IType | null {
        if (t.kind === 'array') {
            const $of = this.getOwnType(t.arrayOf);
            if (!$of) {
                return null;
            }
            return $of.asArray();
        }
        const name = typeSynonyms[t.name] ?? t.name;
        const sizeable = this.sizeableTypes[name];
        if (sizeable) {
            const key = t.config?.length === 1
                ? t.config[0]
                : t.config?.join(',') ?? undefined;
            let ret = sizeable.regs.get(key);
            if (!ret) {
                sizeable.regs.set(key, ret = sizeable.ctor(...t.config ?? []));
            }
            return ret;
        }

        return this.simpleTypes[name] ?? null;
    }


    getTypePub(t: DataType | IType): _IType {
        return this.getType(t as TypeQuery);
    }

    getType(t: TypeQuery): _IType;
    getType(_t: TypeQuery, opts?: QueryObjOpts): _IType | null {
        if (typeof _t === 'number') {
            const byOid = this.relsByTyp.get(_t);
            if (byOid) {
                return asType(byOid);
            }
            throw new TypeNotFound(_t);
        }
        if (typeof _t === 'string') {
            return this.getType({ name: _t });
        }
        if (isType(_t)) {
            return _t;
        }
        const t = _t;
        function chk<T>(ret: T): T {
            if (!ret && !opts?.nullIfNotFound) {
                throw new TypeNotFound(t);
            }
            return ret;
        }
        const schema = schemaOf(t);
        if (schema) {
            if (schema === this.name) {
                return chk(this.getOwnType(t));
            } else {
                return chk(this.db.getSchema(schema)
                    .getType(t, opts));
            }
        }
        if (opts?.skipSearch) {
            return chk(this.getOwnType(t));
        }
        for (const sp of this.db.searchPath) {
            const rel = this.db.getSchema(sp).getOwnType(t);
            if (rel) {
                return rel;
            }
        }
        return chk(this.getOwnType(t));
    }


    getObject(p: QName): _IRelation;
    getObject(p: QName, opts: BeingCreated): _IRelation;
    getObject(p: QName, opts?: QueryObjOpts): _IRelation | null;
    getObject(p: QName, opts?: QueryObjOpts): _IRelation | null {
        function chk(ret: _IRelation | null): _IRelation | null {
            const bc = opts?.beingCreated;
            if (!ret && bc && (
                !p.schema || p.schema === bc.ownerSchema?.name
            ) && bc.name === p.name) {
                ret = bc;
            }
            if (!ret && !opts?.nullIfNotFound) {
                throw new RelationNotFound(p.name);
            }
            return ret;
        }
        if (p.schema) {
            if (p.schema === this.name) {
                return chk(this.getOwnObject(p.name));
            } else {
                return chk(this.db.getSchema(p.schema)
                    .getObject(p, opts));
            }
        }

        if (opts?.skipSearch) {
            return chk(this.getOwnObject(p.name));
        }
        for (const sp of this.db.searchPath) {
            const rel = this.db.getSchema(sp).getOwnObject(p.name);
            if (rel) {
                return rel;
            }
        }
        return chk(this.getOwnObject(p.name));
    }

    getOwnObject(name: string): _IRelation | null {
        return this.relsByNameCas.get(name)
            ?? null;
    }

    getObjectByRegOrName(reg: RegClass): _IRelation;
    getObjectByRegOrName(reg: RegClass, opts?: QueryObjOpts): _IRelation | null;
    getObjectByRegOrName(_reg: RegClass, opts?: QueryObjOpts): _IRelation | null {
        const reg = parseRegClass(_reg);

        if (typeof reg === 'number') {
            return this.getObjectByRegClassId(reg, opts);
        }

        return this.getObject(reg, opts);
    }

    getObjectByRegClassId(reg: number): _IRelation;
    getObjectByRegClassId(reg: number, opts?: QueryObjOpts): _IRelation | null;
    getObjectByRegClassId(reg: number, opts?: QueryObjOpts) {
        function chk<T>(ret: T): T {
            if (!ret && !opts?.nullIfNotFound) {
                throw new RelationNotFound(reg.toString());
            }
            return ret;
        }
        if (opts?.skipSearch) {
            return chk(this.getOwnObjectByRegClassId(reg));
        }
        for (const sp of this.db.searchPath) {
            const rel = this.db.getSchema(sp).getOwnObjectByRegClassId(reg);
            if (rel) {
                return rel;
            }
        }
        return chk(this.getOwnObjectByRegClassId(reg));
    }

    getOwnObjectByRegClassId(reg: number): _IRelation | null {
        return this.relsByCls.get(reg)
            ?? null;
    }

    executeAlterRequest(t: _Transaction, p: AlterTableStatement): QueryResult {
        const table = asTable(this.getObject(p.table));

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

    executeCreateIndex(t: _Transaction, p: CreateIndexStatement): QueryResult {

        // check that index algorithm is supported
        const indexName = p.indexName?.name;
        const onTable = asTable(this.getObject(p.table));
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


    private simple(op: string, p: Statement): QueryResult {
        return {
            command: op,
            fields: [],
            rowCount: 0,
            rows: [],
            location: this.locOf(p),
        };
    }

    executeCreateSequence(t: _Transaction, p: CreateSequenceStatement): QueryResult {
        const name: QName = p.name;
        if ((name.schema ?? this.name) !== this.name) {
            const sch = this.db.getSchema(name.schema) as DbSchema;
            return sch.executeCreateSequence(t, p);
        }

        const ret = this.simple('CREATE', p);

        // check existence
        return this.checkExistence(ret, name, p.ifNotExists, () => {
            if (p.temp) {
                throw new NotSupported('temp sequences');
            }
            new Sequence(name.name, this)
                .alter(t, p.options);
            this.db.onSchemaChange();
        });
    }

    createSequence(t: _Transaction, opts: CreateSequenceOptions | nil, _name: QName | nil): _ISequence {
        _name = _name ?? {
            name: randomString(),
        };
        if ((_name.schema ?? this.name) !== this.name) {
            return this.db.getSchema(_name.schema)
                .createSequence(t, opts, _name);
        }
        const name = _name.name;

        let ret: _ISequence;
        this.checkExistence(null, _name, false, () => {
            ret = new Sequence(name, this)
                .alter(t, opts);
            this.db.onSchemaChange();
        });
        return ret!;
    }

    executeAlterSequence(t: _Transaction, p: AlterSequenceStatement): QueryResult {

        const nop = this.simple('ALTER', p);

        const got = asSeq(this.getObject(p.name, {
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


        const got = asIndex(this.getObject(p.name, {
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

        const got = asTable(this.getObject(p.name, {
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

        const got = asSeq(this.getObject(p.name, {
            nullIfNotFound: p.ifExists,
        }));

        if (!got) {
            nop.ignored = true;
            return nop;
        }

        got.drop(t);
        return nop;
    }


    executeCreateTable(t: _Transaction, p: CreateTableStatement): QueryResult {
        const name: QName = p.name;
        if ((name.schema ?? this.name) !== this.name) {
            const sch = this.db.getSchema(name.schema) as DbSchema;
            return sch.executeCreateTable(t, p);
        }
        const ret = this.simple('CREATE', p);

        return this.checkExistence(ret, name, p.ifNotExists, () => {
            let fields: SchemaField[] = [];
            for (const f of p.columns) {
                switch (f.kind) {
                    case 'column':
                        // TODO: #collation
                        ignore(f.collate);
                        const nf = {
                            ...f,
                            name: f.name.name,
                            type: this.getType(f.dataType),
                            serial: !f.dataType.kind && (f.dataType.name === 'serial' || f.dataType.name === 'bigserial'),
                        };
                        delete (nf as Optional<typeof nf>).dataType;
                        fields.push(nf);
                        break;
                    case 'like table':
                        throw new NotSupported('"like table" statement');
                    default:
                        throw NotSupported.never(f);
                }
            }

            // perform creation
            this.declareTable({
                name: name.name,
                constraints: p.constraints,
                fields,
            });
        });
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

    private executeDelete(t: _Transaction, p: DeleteStatement): WithableResult {
        const table = asTable(this.getObject(p.from));
        const toDelete = table
            .selection
            .filter(p.where);
        const rows = [];
        for (const item of toDelete.enumerate(t)) {
            table.delete(t, item);
            rows.push(item);
        }
        cleanResults(rows);
        return p.returning
            ? buildSelection(new ArrayFilter(table.selection, rows), p.returning)
            : rows.length;
    }

    executeTruncateTable(t: _Transaction, p: TruncateTableStatement): QueryResult {
        if (p.tables.length !== 1) {
            throw new NotSupported('Multiple truncations');
        }
        const table = asTable(this.getObject(p.tables[0]));
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


    private buildValues(p: ValuesStatement, acceptDefault?: boolean): _ISelection {
        const ret = new ValuesTable(this, '', p.values, null, acceptDefault);
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
                    newT = new ValuesTable(this, fnName, [[from]], [fnName])
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
        sel = sel ?? this.dualTable.selection;
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


    getSelectable(name: QNameMapped): _ISelection<any> {
        const temp = !name.schema
            && this.tempBindings.get(name.name);
        if (temp === 'no returning') {
            throw new QueryError(`WITH query "${name.name}" does not have a RETURNING clause`);
        }
        let ret = temp || asSelectable(this.getObject(name)).selection;

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

    private executeUpdate(t: _Transaction, p: UpdateStatement): WithableResult {
        const into = asTable(this.getObject(p.table));

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

        return returning ?? rowCount;
    }

    private createSetter(t: _Transaction, setTable: _ITable, setSelection: _ISelection, _sets: SetStatement[]) {

        const sets = _sets.map(x => {
            const col = (setTable as MemoryTable).getColumnRef(x.column.name);
            return {
                col,
                value: x.value,
                getter: x.value.type !== 'default'
                    ? buildValue(setSelection, x.value).cast(col.expression.type)
                    : null,
            };
        });

        return (target: any, source: any) => {
            for (const s of sets) {
                if (s.value.type === 'default') {
                    target[s.col.expression.id!] = s.col.default?.get() ?? undefined;
                } else {
                    target[s.col.expression.id!] = s.getter?.get(source, t) ?? null;
                }
            }
        }
    }

    private executeInsert(t: _Transaction, p: InsertStatement): WithableResult {
        if (p.type !== 'insert') {
            throw new NotSupported();
        }

        // get table to insert into
        const table = asTable(this.getObject(p.into));
        const selection = table
            .selection
            .setAlias(p.into.alias);


        const ret: any[] = [];
        const returning = p.returning && buildSelection(new ArrayFilter(selection, ret), p.returning);





        const valueRawSource = p.insert.type === 'values'
            ? this.buildValues(p.insert, true)
            : this.buildSelect(p.insert);

        // check not inserting too many values
        const columns: string[] = p.columns?.map(x => x.name)
            ?? table.selection.columns.map(x => x.id!)
                .slice(0, valueRawSource.columns.length);
        if (valueRawSource.columns.length > columns.length) {
            throw new QueryError(`INSERT has more expressions than target columns`);
        }


        // check insert types
        const valueConvertedSource = columns.map((col, i) => {
            const value = valueRawSource.columns[i];
            const insertInto = table.selection.getColumn(col);
            // It seems that the explicit conversion is only performed when inserting values.
            const canConvert = p.insert.type === 'values'
                ? value.type.canCast(insertInto.type)
                : value.type.canConvertImplicit(insertInto.type);
            if (!canConvert) {
                throw new QueryError(`column "${col}" is of type ${insertInto.type.name} but expression is of type ${value.type.name}`);
            }
            return value.type === Types.default
                ? value  // handle "DEFAULT" values
                : value.cast(insertInto.type);
        });

        // enumerate & get
        const values: any[][] = [];
        for (const o of valueRawSource.enumerate(t)) {
            const nv = [];
            for (let i = 0; i < columns.length; i++) {
                const _custom = valueConvertedSource[i].get(o, t);
                nv.push(_custom);
            }
            values.push(nv);
        }




        // build 'on conflict' strategy
        let ignoreConflicts: OnConflictHandler | nil = undefined;
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
                    , {
                        type: 'LEFT JOIN',
                        on: { type: 'boolean', value: false }
                    }
                    , false
                );
                const setter = this.createSetter(t, table, subject, p.onConflict.do.sets,);
                const where = p.onConflict.where && buildValue(subject, p.onConflict.where);
                ignoreConflicts = {
                    onIndex,
                    update: (item, excluded) => {
                        // build setter context
                        const jitem = subject.buildItem(item, excluded);

                        // check "WHERE" clause on conflict
                        if (where) {
                            const whereClause = where.get(jitem, t);
                            if (whereClause !== true) {
                                return;
                            }
                        }

                        // execute set
                        setter(item, jitem);
                    },
                }
            }
        }

        // insert values
        let rowCount = 0;
        const opts: ChangeOpts = {
            onConflict: ignoreConflicts,
            overriding: p.overriding
        };
        for (const val of values) {
            rowCount++;
            if (val.length !== columns.length) {
                throw new QueryError('Insert columns / values count mismatch');
            }
            const toInsert: any = {};
            for (let i = 0; i < val.length; i++) {
                const v = val[i];
                const col = valueConvertedSource[i];
                if (col.type === Types.default) {
                    continue; // insert a 'DEFAULT' value
                }
                toInsert[columns[i]] = v;
                // if ('_custom' in v) {
                //      toInsert[columns[i]] = v._custom;
                // } else {
                //     const notConv = buildValue(table.selection, v);
                //     const converted = notConv.cast(col.type);
                //     if (!converted.isConstant) {
                //         throw new QueryError('Cannot insert non constant expression');
                //     }
                //     toInsert[columns[i]] = converted.get();
                // }
            }
            ret.push(table.doInsert(t, toInsert, opts));
        }

        return returning ?? rowCount;
    }


    getTable(name: string): _ITable;
    getTable(name: string, nullIfNotFound?: boolean): _ITable | null;
    getTable(name: string, nullIfNotFound?: boolean): _ITable | null {
        const ret = this.getOwnObject(name);
        if ((!ret || ret.type !== 'table')) {
            if (nullIfNotFound) {
                return null;
            }
            throw new RelationNotFound(name);
        }
        return ret;
    }



    declareTable(table: Schema, noSchemaChange?: boolean): MemoryTable {
        const trans = this.db.data.fork();
        const ret = new MemoryTable(this, trans, table).register();
        trans.commit();
        if (!noSchemaChange) {
            this.db.onSchemaChange();
        }
        return ret;
    }

    registerEquivalentType(type: IEquivalentType): IType {
        const ret = new EquivalentType(type);
        this._registerType(ret);
        return ret;
    }

    _registerTypeSizeable(name: string, ctor: (sz?: number) => _IType): this {
        if (this.simpleTypes[name] || this.sizeableTypes[name]) {
            throw new QueryError(`type "${name}" already exists`);
        }
        this.sizeableTypes[name] = {
            ctor,
            regs: new Map(),
        };
        return this;
    }

    _registerType(type: _IType): this {
        if (this.simpleTypes[type.primary] || this.sizeableTypes[type.primary] || this.getOwnObject(type.primary)) {
            throw new QueryError(`type "${type.primary}" already exists`);
        }
        this.simpleTypes[type.primary] = type;
        this._reg_register(type);
        return this;
    }


    _reg_register(rel: _IRelation): Reg {
        if (this.readonly) {
            throw new PermissionDeniedError()
        }
        if (this.relsByNameCas.has(rel.name)) {
            throw new Error(`relation "${rel.name}" already exists`);
        }
        const ret: Reg = regGen();
        this.relsByNameCas.set(rel.name, rel);
        this.relsByCls.set(ret.classId, rel);
        this.relsByTyp.set(ret.typeId, rel);
        if (rel.type === 'table') {
            this._tables.add(rel);
        }
        return ret;
    }

    _reg_unregister(rel: _IRelation): void {
        if (this.readonly) {
            throw new PermissionDeniedError()
        }
        this.relsByNameCas.delete(rel.name);
        this.relsByCls.delete(rel.reg.classId);
        this.relsByTyp.delete(rel.reg.typeId);
        if (rel.type === 'table') {
            this._tables.delete(rel);
        }
    }

    _reg_rename(rel: _IRelation, oldName: string, newName: string): void {
        if (this.readonly) {
            throw new PermissionDeniedError()
        }
        if (this.relsByNameCas.has(newName)) {
            throw new Error('relation exists: ' + newName);
        }
        if (this.relsByNameCas.get(oldName) !== rel) {
            throw new Error('consistency error while renaming relation');
        }
        this.relsByNameCas.delete(oldName);
        this.relsByNameCas.set(newName, rel);
    }



    tablesCount(t: _Transaction): number {
        return this._tables.size;
    }


    *listTables(): Iterable<_ITable> {
        for (const t of this._tables.values()) {
            if (!t.hidden) {
                yield t;
            }
        }
    }

    registerFunction(fn: FunctionDefinition, replace?: boolean): this {
        let fns = this.fns.get(fn.name);
        if (!fns) {
            this.fns.set(fn.name, fns = []);
        }
        fns.push({
            args: (fn.args?.map<ArgDefDetails>(x => {
                if (typeof x === 'string' || isType(x)) {
                    return {
                        type: this.getTypePub(x),
                    };
                }
                return x;
            }) ?? []) as _ArgDefDetails[],
            argsVariadic: fn.argsVariadic && this.getTypePub(fn.argsVariadic),
            returns: fn.returns && this.getTypePub(fn.returns),
            impure: !!fn.impure,
            implementation: fn.implementation,
            allowNullArguments: fn.allowNullArguments,
        });
        return this;
    }

    getFunctions(name: string | QName, arrity: number | nil, forceOwn?: boolean): Iterable<_FunctionDefinition> {
        const asSingle = asSingleQName(name, this.name);
        if (!asSingle || !forceOwn) {
            return this.db.getFunctions(name, arrity);
        }
        const matches = this.fns.get(asSingle);
        return !matches || nullIsh(arrity)
            ? matches ?? []
            : matches.filter(m => m.args.length === arrity
                || m.args.length < arrity! && m.argsVariadic);
    }


    async migrate(config?: IMigrate.MigrationParams) {
        await migrate(this, config);
    }



    interceptQueries(intercept: QueryInterceptor): ISubscription {
        const qi = { intercept } as const;
        this.interceptors.add(qi);
        return {
            unsubscribe: () => {
                this.interceptors.delete(qi);
            }
        };
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
