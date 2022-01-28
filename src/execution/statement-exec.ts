import { watchUse, ignore, errorMessage } from '../utils';
import { _ISchema, _Transaction, _FunctionDefinition, _ArgDefDetails, _IType, _ISelection, _IStatement, NotSupported, QueryError, nil, OnStatementExecuted, _IStatementExecutor, StatementResult } from '../interfaces-private';
import { toSql, Statement, SelectStatement, ValuesStatement } from 'pgsql-ast-parser';
import { ExecuteCreateTable } from './schema-amends/create-table';
import { ExecuteCreateSequence } from './schema-amends/create-sequence';
import { resultNoData, locOf } from './exec-utils';
import { CreateIndexExec } from './schema-amends/create-index';
import { Alter } from './schema-amends/alter';
import { AlterSequence } from './schema-amends/alter-sequence';
import { DropIndex } from './schema-amends/drop-index';
import { DropTable } from './schema-amends/drop-table';
import { DropSequence } from './schema-amends/drop-sequence';
import { CommitExecutor, RollbackExecutor, BeginStatementExec } from './transaction-statements';
import { TruncateTable } from './records-mutations/truncate-table';
import { ShowExecutor } from './show';
import { SetExecutor } from './set';
import { CreateEnum } from './schema-amends/create-enum';
import { CreateView } from './schema-amends/create-view';
import { CreateMaterializedView } from './schema-amends/create-materialized-view';
import { CreateSchema } from './schema-amends/create-schema';
import { CreateFunction } from './schema-amends/create-function';
import { DoStatementExec } from './schema-amends/do';
import { SelectExec } from './select';
import { withSelection, withStatement } from '../parser/context';

const detailsIncluded = Symbol('errorDetailsIncluded');

export class SimpleExecutor implements _IStatementExecutor {
    constructor(private st: Statement, private exec: (t: _Transaction) => void, private opName?: string) {
    }
    execute(t: _Transaction): StatementResult {
        this.exec(t);
        return resultNoData(this.opName ?? this.st.type.toUpperCase(), this.st, t);
    }
}

export class StatementExec implements _IStatement {
    private onExecutedCallbacks: OnStatementExecuted[] = []
    lastSelect?: _ISelection;
    private executor?: _IStatementExecutor;

    constructor(readonly schema: _ISchema, private statement: Statement, private pAsSql: string | nil) {
    }

    onExecuted(callback: OnStatementExecuted): void {
        this.onExecutedCallbacks.push(callback);
    }

    private get db() {
        return this.schema.db;
    }


    compile(): _IStatementExecutor {
        if (this.executor) {
            return this.executor!;
        }
        const _p = this.statement;
        // build the AST coverage checker
        const { checked: p, check } = this.db.options.noAstCoverageCheck
            ? { checked: _p, check: null }
            : watchUse(_p);

        // parse the AST
        withStatement(this, () => {
            withSelection(this.schema.dualTable.selection, () => {
                this.executor = this._getExecutor();
            });
        })

        // check AST coverage
        // const err = check?.();
        // if (err) {
        //     throw new NotSupported(err);
        // }

        return this.executor!;
    }


    private _getExecutor(): _IStatementExecutor {
        const p = this.statement;
        switch (p.type) {
            case 'start transaction':
            case 'begin':
                return new BeginStatementExec(p);
            case 'commit':
                return new CommitExecutor(p);
            case 'rollback':
                return new RollbackExecutor(p);
            case 'select':
            case 'delete':
            case 'update':
            case 'insert':
            case 'union':
            case 'union all':
            case 'values':
            case 'with recursive':
            case 'with':
                return new SelectExec(this, p);
            // case 'with':
            // result = this.executeWith(t, p);
            case 'truncate table':
                return new TruncateTable(p);
            case 'create table':
                return new ExecuteCreateTable(this.schema, p);
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
            case 'show':
                return new ShowExecutor(p);
            case 'set':
            case 'set timezone':
                return new SetExecutor(p);
            case 'create enum':
                return new CreateEnum(this, p);
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
            case 'raise':
                ignore(p);
                return new SimpleExecutor(p, () => { });

            case 'tablespace':
                throw new NotSupported('"TABLESPACE" statement');
            case 'prepare':
                throw new NotSupported('"PREPARE" statement');
            case 'create composite type':
                throw new NotSupported('create composite type');
            default:
                throw NotSupported.never(p, 'statement type');
        }
    }


    private execute(t: _Transaction) {
        this.compile();
        const ret = this.executor!.execute(t);
        for (const s of this.onExecutedCallbacks) {
            s(t);
        }
        return ret;
    }


    executeStatement(t: _Transaction): StatementResult {
        try {

            t.clearTransientData();

            return this.execute(t);
        } catch (e) {

            // handle reeantrant calls (avoids including error tips twice)
            if (e && typeof e === 'object' && e[detailsIncluded]) {
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
                e[detailsIncluded] = true;
            }
            throw e;
        }
    }
}