import { watchUse, ignore, errorMessage, pushExecutionCtx, fromEntries } from '../utils';
import { _ISchema, _Transaction, _FunctionDefinition, _ArgDefDetails, _IType, _ISelection, _IStatement, NotSupported, QueryError, nil, OnStatementExecuted, _IStatementExecutor, StatementResult, Parameter, IValue, PreparedStatementRunner, _IPreparedQuery } from '../interfaces-private';
import { toSql, Statement } from 'pgsql-ast-parser';
import { ExecuteCreateTable } from './schema-amends/create-table';
import { ExecuteCreateSequence } from './schema-amends/create-sequence';
import { locOf, ExecHelper } from './exec-utils';
import { CreateIndexExec } from './schema-amends/create-index';
import { Alter } from './schema-amends/alter';
import { AlterSequence } from './schema-amends/alter-sequence';
import { DropIndex } from './schema-amends/drop-index';
import { AlterIndex } from './schema-amends/alter-index';
import { DropTable } from './schema-amends/drop-table';
import { DropSequence } from './schema-amends/drop-sequence';
import { CommitExecutor, RollbackExecutor, BeginStatementExec, SavepointExecutor, ReleaseSavepointExecutor } from './transaction-statements';
import { TruncateTable } from './records-mutations/truncate-table';
import { ShowExecutor } from './show';
import { SetExecutor } from './set';
import { CreateRoleExecutor, DropRoleExecutor, SetRoleExecutor, ResetExecutor } from './roles';
import { CreatePolicy, DropPolicy } from './schema-amends/create-policy';
import { CreateTrigger } from './schema-amends/create-trigger';
import { CreateEnum } from './schema-amends/create-enum';
import { CreateView } from './schema-amends/create-view';
import { CreateMaterializedView } from './schema-amends/create-materialized-view';
import { CreateSchema } from './schema-amends/create-schema';
import { CreateFunction } from './schema-amends/create-function';
import { DoStatementExec } from './schema-amends/do';
import { SelectExec } from './select';
import { withSelection, withStatement, withNameResolver, INameResolver } from '../parser/context';
import { DropType } from './schema-amends/drop-type';
import { AlterEnum } from "./schema-amends/alter-enum";
import { Comment } from './schema-amends/comment';
import { ExecutePrepared } from './execute-prepared';
import { CreateDomain } from './schema-amends/create-domain';
import { CreateCompositeType } from './schema-amends/create-composite-type';
import { InsteadOfView } from './records-mutations/instead-of';
import { MergeExec } from './records-mutations/merge';
import { hasInsteadOf, TriggerOp } from './triggers';
import { _IView, _ITable } from '../interfaces-private';

const detailsIncluded = Symbol('errorDetailsIncluded');

export class SimpleExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(st: Statement, private exec: (t: _Transaction) => void, private opName?: string) {
        super(st);
    }
    execute(t: _Transaction): StatementResult {
        this.exec(t);
        return this.noData(t, this.opName);
    }
}

class MapNameResolver implements INameResolver {
    constructor(private map: Map<string, any>, readonly isolated: boolean) {
    }
    resolve(name: string): IValue | nil {
        return this.map.get(name);
    }
}

export class StatementExec implements _IStatement {
    private onExecutedCallbacks: OnStatementExecuted[] = []
    private executor?: _IStatementExecutor;
    private checkAstCoverage?: (() => void);

    constructor(readonly schema: _ISchema, private statement: Statement, private pAsSql: string | nil, private parameters?: Parameter[]) {
    }

    onExecuted(callback: OnStatementExecuted): void {
        this.onExecutedCallbacks.push(callback);
    }

    private get db() {
        return this.schema.db;
    }


    private _getExecutor(p: Statement): _IStatementExecutor {
        switch (p.type) {
            case 'start transaction':
            case 'begin':
                return new BeginStatementExec(p);
            case 'commit':
                return new CommitExecutor(p);
            case 'rollback':
                return new RollbackExecutor(p);
            case 'savepoint':
                return new SavepointExecutor(p);
            case 'release savepoint':
                return new ReleaseSavepointExecutor(p);
            case 'delete':
            case 'update':
            case 'insert': {
                // DML on a view with an INSTEAD OF trigger fires the trigger instead
                const io = this.insteadOfViewExecutor(p);
                if (io) { return io; }
                return new SelectExec(this, p);
            }
            case 'merge':
                return new MergeExec(this, p);
            case 'select':
            case 'union':
            case 'union all':
            case 'intersect':
            case 'intersect all':
            case 'except':
            case 'except all':
            case 'values':
            case 'with recursive':
            case 'with':
                return new SelectExec(this, p);
            case 'truncate table':
                return new TruncateTable(p);
            case 'create table':
                return new ExecuteCreateTable(p);
            case 'create index':
                return new CreateIndexExec(this, p);
            case 'alter table':
                return new Alter(this, p);
            case 'create extension':
                return new SimpleExecutor(p, () => this.schema.executeCreateExtension(p));
            case 'create sequence':
                return new ExecuteCreateSequence(this.schema, p, false);
            case 'alter sequence':
                return new AlterSequence(this, p);
            case 'drop index':
                return new DropIndex(this, p);
            case 'drop table':
                return new DropTable(this, p);
            case 'drop sequence':
                return new DropSequence(this, p);
            case 'drop type':
                return new DropType(this, p);
            case 'show':
                return new ShowExecutor(p);
            case 'set':
            case 'set names':
            case 'set timezone':
                return new SetExecutor(p);
            case 'create role':
                return new CreateRoleExecutor(p);
            case 'drop role':
                return new DropRoleExecutor(p);
            case 'set role':
                return new SetRoleExecutor(p);
            case 'reset':
                return new ResetExecutor(p);
            case 'create policy':
                return new CreatePolicy(this, p);
            case 'drop policy':
                return new DropPolicy(this, p);
            case 'create trigger':
                return new CreateTrigger(this, p);
            case 'create enum':
                return new CreateEnum(this, p);
            case 'alter enum':
                return new AlterEnum(this, p)
            case 'create view':
                return new CreateView(this, p);
            case 'create materialized view':
                return new CreateMaterializedView(this, p);
            case 'create schema':
                return new CreateSchema(this, p);
            case 'create function':
                return new CreateFunction(this, p);
            case 'drop function':
                return new SimpleExecutor(p, () => this.schema.dropFunction(p), 'DROP');
            case 'do':
                return new DoStatementExec(this, p);
            case 'comment':
                return new Comment(this, p);
            case 'raise':
            case 'grant':
            case 'revoke':
                // pg-mem has no privilege system: parse & ignore (dumps, RLS setup)
                ignore(p);
                return new SimpleExecutor(p, () => { });

            case 'deallocate':
                ignore(p.target);
                return new SimpleExecutor(p, () => {
                    const tgt = p.target;
                    if ('option' in tgt) {
                        // DEALLOCATE ALL
                        this.db.preparedStatements.clear();
                    } else if (!this.db.preparedStatements.delete(tgt.name)) {
                        throw new QueryError(`prepared statement "${tgt.name}" does not exist`, '26000');
                    }
                }, 'DEALLOCATE');

            case 'refresh materialized view':
                // todo: a decent materialized view implementation
                ignore(p);
                return new SimpleExecutor(p, () => { });

            case 'tablespace':
                // tablespaces are physical storage; meaningless in-memory
                ignore(p);
                return new SimpleExecutor(p, () => { });
            case 'prepare': {
                // plan the statement now (like postgres: at PREPARE time), outside any
                // execution context, then stash a runner. The raw (unproxied) inner AST is
                // used so it gets its own coverage checking inside schema.prepare.
                ignore(p.statement);
                ignore(p.args);
                const raw = this.statement as typeof p;
                const compiled = this.schema.prepare([raw.statement]) as _IPreparedQuery;
                const runner: PreparedStatementRunner = (args, t) => {
                    const res = compiled.bind(args).executeAll(t) as any;
                    const { state, ...result } = res;
                    return { result, state };
                };
                return new SimpleExecutor(p, () => {
                    if (this.db.preparedStatements.has(p.name.name)) {
                        throw new QueryError(`prepared statement "${p.name.name}" already exists`, '42P05');
                    }
                    this.db.preparedStatements.set(p.name.name, runner);
                }, 'PREPARE');
            }
            case 'execute':
                return new ExecutePrepared(this, p);
            case 'create composite type':
                return new CreateCompositeType(this, p);
            case 'create domain':
                return new CreateDomain(this, p);
            case 'drop trigger':
                // DROP TRIGGER <name> ON <table|view> [IF EXISTS]
                return new SimpleExecutor(p, () => {
                    const dp = p as any;
                    const obj = this.schema.getThisOrSiblingFor(dp.onTable)
                        .getObject(dp.onTable, { nullIfNotFound: !!dp.ifExists });
                    if (!obj) {
                        return; // IF EXISTS: unknown relation is a no-op
                    }
                    if (obj.type !== 'table' && obj.type !== 'view') {
                        throw new QueryError(`"${dp.onTable.name}" is not a table or view`);
                    }
                    (obj as _ITable | _IView).dropTrigger(dp.name.name, !!dp.ifExists);
                }, 'DROP TRIGGER');
            case 'alter index':
                return new AlterIndex(this, p);
            default:
                throw NotSupported.never(p, 'statement type');
        }
    }

    /** if this DML targets a view with a matching INSTEAD OF trigger, an executor for it */
    private insteadOfViewExecutor(p: Statement): _IStatementExecutor | null {
        const target = p.type === 'insert' ? p.into
            : p.type === 'update' ? p.table
                : (p as any).from;
        if (!target || target.type === 'statement') {
            return null;
        }
        const obj = this.schema.getObject(target, { nullIfNotFound: true });
        if (obj?.type !== 'view') {
            return null;
        }
        const op = p.type as TriggerOp;
        if (!hasInsteadOf(obj as _IView, op)) {
            return null;
        }
        return new InsteadOfView(obj as _IView, p as any);
    }




    compile(): _IStatementExecutor {
        return this.niceErrors(() => {

            if (this.executor) {
                return this.executor!;
            }
            // build the AST coverage checker
            let p = this.statement;
            if (!this.db.options.noAstCoverageCheck) {
                const watched = watchUse(p);
                p = watched.checked;
                this.checkAstCoverage = () => {
                    const err = watched.check?.();
                    if (err) {
                        throw new NotSupported(err);
                    }
                };
            }

            // build parameters context
            const namedParams = fromEntries(this.parameters?.filter(p => !!p.value?.id).map(x => [x.value!.id!, x]) ?? []);
            const nameResolver = new MapNameResolver(namedParams, true);


            // parse the AST
            withNameResolver(nameResolver,
                () => withStatement(this,
                    () => withSelection(this.schema.dualTable.selection,
                        () => this.executor = this._getExecutor(p)
                    )
                )
            );

            return this.executor!;
        });
    }


    executeStatement(t: _Transaction, parametersValues: any[]): StatementResult {
        return this.niceErrors(() => pushExecutionCtx({
            transaction: t,
            schema: this.schema,
            parametersValues,
        }, () => {

            t.clearTransientData();

            // actual execution
            if (!this.executor) {
                throw new Error('Statement not prepared')
            }
            const result = this.executor.execute(t);

            // post-execution
            for (const s of this.onExecutedCallbacks) {
                s(t);
            }


            // check AST coverage if necessary
            this.checkAstCoverage?.();


            return result;
        }));
    }

    private niceErrors<T>(act: () => T): T {
        try {
            return act();
        } catch (e) {
            // handle reeantrant calls (avoids including error tips twice)
            if (e && typeof e === 'object' && (e as any)[detailsIncluded]) {
                throw e;
            }

            // include error tips
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

            // set error location
            if (e && typeof e === 'object') {
                (e as any).location = locOf(this.statement);
                (e as any)[detailsIncluded] = true;
            }
            throw e;
        }
    }

}
