import { ISchema, DataType, IType, RelationNotFound, Schema, QueryResult, SchemaField, nil, FunctionDefinition, PermissionDeniedError, TypeNotFound, ArgDefDetails, IEquivalentType, QueryInterceptor, ISubscription, QueryError, typeDefToStr, OperatorDefinition, QueryOrAst } from '../interfaces';
import { _IDb, _ISelection, _ISchema, _Transaction, _ITable, _SelectExplanation, _Explainer, IValue, _IIndex, _IType, _IRelation, QueryObjOpts, _ISequence, _INamedIndex, RegClass, Reg, TypeQuery, asType, _ArgDefDetails, BeingCreated, _FunctionDefinition, _OperatorDefinition } from '../interfaces-private';
import { asSingleQName, ignore, isType, parseRegClass, randomString, schemaOf } from '../utils';
import { typeSynonyms } from '../datatypes';
import { DropFunctionStatement, BinaryOperator, QName, DataTypeDef, CreateSequenceOptions, CreateExtensionStatement, Statement } from 'pgsql-ast-parser';
import { MemoryTable } from '../table';
import { parseSql } from '../parser/parse-cache';
import { migrate } from '../migrate/migrate';
import { CustomEnumType } from '../datatypes/t-custom-enum';
import { regGen } from '../datatypes/datatype-base';
import { EquivalentType } from '../datatypes/t-equivalent';
import { OverloadResolver } from './overload-resolver';
import { ExecuteCreateSequence } from '../execution/schema-amends/create-sequence';
import { StatementExec } from '../execution/statement-exec';
import { SelectExec } from '../execution/select';
import { MigrationParams } from '../migrate/migrate-interfaces';

export class DbSchema implements _ISchema, ISchema {

    readonly dualTable: _ITable;
    private relsByNameCas = new Map<string, _IRelation>();
    private relsByCls = new Map<number, _IRelation>();
    private relsByTyp = new Map<number, _IRelation>();
    private _tables = new Set<_ITable>();

    private fns = new OverloadResolver<_FunctionDefinition>(false);
    private ops = new OverloadResolver<_OperatorDefinition>(false);
    private installedExtensions = new Set<string>();
    private readonly: any;
    private interceptors = new Set<{ readonly intercept: QueryInterceptor }>();
    private lastSelect?: _ISelection;

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


    none(query: QueryOrAst): void {
        this.query(query);
    }

    one(query: QueryOrAst): any {
        const [result] = this.many(query);
        return result;
    }

    many(query: QueryOrAst): any[] {
        return this.query(query).rows;
    }


    query(text: QueryOrAst): QueryResult {
        // intercept ?
        if (typeof text === 'string') {
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
        }

        // execute.
        let last: QueryResult | undefined;
        for (const r of this.queries(text)) {
            last = r;
        }
        return last ?? {
            command: typeof text === 'string' ? text : '<custom ast>',
            fields: [],
            location: { start: 0, end: typeof text === 'string' ? text.length : 0 },
            rowCount: 0,
            rows: [],
        };
    }

    private parse(query: QueryOrAst): Statement[] {
        if (typeof query === 'string') {
            return parseSql(query);
        }
        return Array.isArray(query) ? query : [query];
    }

    *queries(query: QueryOrAst): Iterable<QueryResult> {
        query = typeof query === 'string' ? query + ';' : query;
        try {

            // Parse statements
            let parsed = this.parse(query);
            if (!Array.isArray(parsed)) {
                parsed = [parsed];
            }
            const singleSql = typeof query === 'string' && parsed.length === 1 ? query : undefined;

            // Prepare statements
            const prepared = parsed
                .filter(s => !!s)
                .map(x => new StatementExec(this, x, singleSql));

            // Start an implicit transaction
            //  (to avoid messing global data if an operation fails mid-write)
            let t = this.db.data.fork();

            // Execute statements
            for (const p of prepared) {

                // Prepare statement
                const executor = p.compile();

                // store last select for debug purposes
                if (executor instanceof SelectExec) {
                    this.lastSelect = executor.selection;
                }

                // Execute statement
                const { state, result } = p.executeStatement(t);
                yield result;
                t = state;
            }

            // implicit final commit
            t.fullCommit();
            this.db.raiseGlobal('query', query);
        } catch (e) {
            this.db.raiseGlobal('query-failed', query);
            throw e;
        }
    }



    registerEnum(name: string, values: string[]) {
        new CustomEnumType(this, name, values).install();
    }

    getThisOrSiblingFor(name: QName): _ISchema {
        if (!name?.schema || name.schema === this.name) {
            return this;
        }
        return this.db.getSchema(name.schema);
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
        // fetch synonyms
        const synonym = t.doubleQuoted ? null : typeSynonyms[t.name];
        let name: string;
        let ignoreConfig = false;
        if (typeof synonym === 'object' && synonym && 'type' in synonym) {
            name = synonym.type;
            ignoreConfig = synonym.ignoreConfig;
        } else {
            name = synonym ?? t.name;
        }
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
        } else if (ignoreConfig) {
            ignore(t.config);
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

    createSequence(t: _Transaction, opts: CreateSequenceOptions | nil, _name: QName | nil): _ISequence {
        _name = _name ?? {
            name: randomString(),
        };
        return new ExecuteCreateSequence(this, {
            type: 'create sequence',
            name: _name,
            options: opts ?? {},
        }, true).createSeq(t)!;
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
        const prepared = new StatementExec(this, parsed[0], sql)
            .compile();
        if (!(prepared instanceof SelectExec)) {
            throw new Error('Can only explain selection executors');
        }
        return prepared
            .selection
            .explain(new Explainer(this.db.data))
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

    _unregisterType(type: _IType): this {
        delete this.simpleTypes[type.primary];
        this._reg_unregister(type);
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
        const def: _FunctionDefinition = {
            name: fn.name,
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
        };

        this.fns.add(def, replace ?? true);
        return this;
    }

    registerOperator(op: OperatorDefinition, replace?: boolean): this {
        this._registerOperator(op, replace ?? true);
        if (op.commutative && op.left !== op.right) {
            this._registerOperator({
                ...op,
                left: op.right,
                right: op.left,
                implementation: (a, b) => op.implementation(b, a),
            }, replace ?? true);
        }
        return this;
    }

    private _registerOperator(fn: OperatorDefinition, replace: boolean): this {
        const args = [fn.left, fn.right].map<ArgDefDetails>(x => {
            if (typeof x === 'string' || isType(x)) {
                return {
                    type: this.getTypePub(x),
                };
            }
            return x;
        }) as _ArgDefDetails[];
        const def: _OperatorDefinition = {
            name: fn.operator,
            args,
            left: args[0].type,
            right: args[1].type,
            returns: fn.returns && this.getTypePub(fn.returns),
            impure: !!fn.impure,
            implementation: fn.implementation,
            allowNullArguments: fn.allowNullArguments,
            commutative: fn.commutative ?? false,
        };

        this.ops.add(def, replace);
        return this;
    }


    resolveFunction(name: string | QName, args: IValue[], forceOwn?: boolean): _FunctionDefinition | nil {
        const asSingle = asSingleQName(name, this.name);
        if (!asSingle || !forceOwn) {
            return this.db.resolveFunction(name, args);
        }
        return this.fns.resolve(asSingle, args);
    }

    getFunction(name: string, args: _IType[]): _FunctionDefinition | nil {
        return this.fns.getExact(name, args);
    }


    dropFunction(fn: DropFunctionStatement): void {
        if (fn.name.schema && fn.name.schema !== this.name) {
            return (this.db.getSchema(fn.name.schema) as DbSchema).dropFunction(fn);
        }
        const fns = this.fns.getOverloads(fn.name.name);

        // === determine which function to delete
        let toRemove: _FunctionDefinition;
        if (fn.arguments) {
            const targetArgs = fn.arguments;
            const match = fns?.filter(x => x.args.length === targetArgs.length
                && !x.args.some((a, i) => a.type !== this.getType(targetArgs[i].type)));
            if (!match?.length) {
                if (fn.ifExists) {
                    return;
                }
                throw new QueryError(`function ${fn.name.name}(${targetArgs.map(t => typeDefToStr(t.type)).join(',')}) does not exist`, '42883');
            }
            if (match.length > 1) {
                throw new QueryError(`function name "${fn.name.name}" is ambiguous`, '42725');
            }
            toRemove = match[0];
        } else {
            if (!fns?.length) {
                if (fn.ifExists) {
                    return;
                }
                throw new QueryError(`could not find a function named "${fn.name.name}"`, '42883');
            }
            if (fns?.length !== 1) {
                throw new QueryError(`function name "${fn.name.name}" is not unique`, '42725');
            }
            toRemove = fns[0];
        }


        this.fns.remove(toRemove);
    }

    resolveOperator(name: BinaryOperator, left: IValue, right: IValue, forceOwn?: boolean): _OperatorDefinition | nil {
        if (!forceOwn) {
            return this.db.resolveOperator(name, left, right);
        }
        return this.ops.resolve(name, [left, right]);
    }


    async migrate(config?: MigrationParams) {
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

    idFor(sel: _ISelection): string | number {
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
