import { astVisitor, DataTypeDef, Expr, Statement } from 'pgsql-ast-parser';
import { _IDb, _ISchema, _IStatementExecutor, _Transaction, FieldInfo, IBoundQuery, IPreparedQuery, nil, NotSupported, QueryError, QueryResult, StatementResult } from '../interfaces-private';
import { StatementExec } from '../execution/statement-exec';
import { SelectExec } from '../execution/select';
import { nullIsh } from '../utils';
import moment from 'moment';

function isSchemaChange(s: Statement): boolean {
    switch (s.type) {
        case 'alter enum':
        case 'alter index':
        case 'alter sequence':
        case 'alter table':
        case 'create composite type':
        case 'create enum':
        case 'create extension':
        case 'create function':
        case 'create index':
        case 'create materialized view':
        case 'create schema':
        case 'create sequence':
        case 'create table':
        case 'create view':
        case 'drop function':
        case 'drop index':
        case 'drop sequence':
        case 'drop table':
        case 'drop trigger':
        case 'drop type':
            return true;

        case 'do':
            // we dont know what's in there
            //  => assume it could be schema change
            return true;
        default:
            return false;
    }
}

export type _IPreparedQuery = IPreparedQuery & {
    executed?: () => void;
    failed?: (e: any) => void;
}

let _hasParam = false;
const hasParamVisitor = astVisitor(() => ({
    parameter: () => { _hasParam = true }
}))

function hasParameter(stmt: Statement): boolean {
    _hasParam = false;
    hasParamVisitor.statement(stmt);
    return _hasParam;
}

export function prepareQuery(schema: _ISchema, query: Statement[], singleSql: string | nil): _IPreparedQuery {

    const hasParam = query.some(hasParameter);

    // pg-mem does not support preparing statements that will trigger schema changes
    //  suppose we have:
    //   - a create table
    //   - a select on this table
    // compiling the second statement before executing the first will fail
    //  supporting this would require pg-mem to to implement transactions for schema changes,
    //  which is no small feat (and frankly, not a priority)
    // => when there is a schema change, we fallback to a PreparedQueryNoDescribe
    //   which compiles AND executes statements one by-one, commiting things each time it executes a schema change
    //   and thus does not allow describing the operation before executing it
    const ret = query.some(isSchemaChange)
        ? new PreparedQueryNoDescribe(schema, query, singleSql)
        : new PreparedQuery(schema, query, singleSql);

    return ret;
}

class PreparedQueryNoDescribe implements IPreparedQuery {

    executed?: () => void;
    failed?: (e: any) => void;

    constructor(readonly schema: _ISchema, private query: Statement[], private singleSql: string | nil) {
    }

    describe(): FieldInfo[] {
        const l = this.query[this.query.length - 1];
        if (!isSchemaChange(l)) {
            // see comment in prepareQuery
            throw new NotSupported('describe a schema-change query');
        }
        return [];
    }

    bind(...args: any[]): IBoundQuery {
        // Start an implicit transaction
        let t = this.schema.db.data.fork();
        const results: StatementResult[] = [];
        for (const stmt of this.query) {
            const s = new StatementExec(this.schema, stmt, this.singleSql);

            const compiled = s.compile();

            // store last select for debug purposes
            if (compiled instanceof SelectExec) {
                this.schema.lastSelect = compiled.selection;
            }

            // execute statement
            const r = s.executeStatement(t);
            results.push(r);
            t = r.state;
        }
        return new NoDescribeBound(this, results);
    }
}

class NoDescribeBound implements IBoundQuery {
    constructor(private parent: PreparedQueryNoDescribe, private results: StatementResult[]) {
    }
    executeAll(): QueryResult {
        const last = this.results[this.results.length - 1]!;
        last.state.fullCommit();
        this.parent.executed?.();
        return last.result;
    }

    *iterate(): IterableIterator<QueryResult> {
        for (const res of this.results) {
            yield res.result;
        }
        this.executeAll();
    }
}

class PreparedQuery implements IPreparedQuery {

    private lastSelect: SelectExec | null = null;
    private lastStmt: _IStatementExecutor | null = null;
    private stmts: StatementExec[];
    executed?: () => void;
    failed?: (e: any) => void;

    constructor(readonly schema: _ISchema, query: Statement[], singleSql: string | nil) {
        // there is no schema change, meaning that we can compile statements
        // without fearing that a statement
        const stmts = query.map(s => new StatementExec(schema, s, singleSql));
        for (const stmt of stmts) {
            const compiled = stmt.compile();

            // store last select for debug purposes
            if (compiled instanceof SelectExec) {
                this.lastSelect = compiled;
            }
            this.lastStmt = compiled;
        }

        this.stmts = stmts;
    }

    describe(): FieldInfo[] {
        if (this.lastStmt instanceof SelectExec) {
            return this.lastStmt.selection.columns.map<FieldInfo>(c => ({
                name: c.id!,
                type: c.type.primary,
                typeId: c.type.reg.typeId,
            }));
        }
        return [];
    }

    bind(...args: any[]): Bound {
        // Start an implicit transaction
        //  (to avoid messing global data if an operation fails mid-write)
        let t = this.schema.db.data.fork();

        // // bind parameters
        // let query = this.query;
        // if (args.length) {
        //     const mapper = astMapper(map => ({
        //         parameter: p => {
        //             const [, istr] = /^\$(\d+)$/.exec(p.name) ?? [];
        //             if (!istr) {
        //                 throw new QueryError('Invalid parameter name');
        //             }
        //             const i = parseInt(istr, 10) - 1;
        //             if (i >= args.length) {
        //                 throw new QueryError('Parameter out of range');
        //             }
        //             const value = dataToLiteral(args[i]);
        //             return value;
        //         }
        //     }));
        //     query = this.query.map(stmt => mapper.statement(stmt)!);
        // }



        let results: StatementResult[] = [];
        for (const s of this.stmts) {
            // Execute statement
            const r = s.executeStatement(t);
            results.push(r);
            t = r.state;
        }

        return new Bound(this, results, this.lastSelect);
    }
}

function dataToLiteral(value: unknown): Expr {
    if (Array.isArray(value)) {
        return {
            type: 'array',
            expressions: value.map(dataToLiteral),
        };
    }
    if (nullIsh(value)) {
        return { type: 'null' }
    }
    switch (typeof value) {
        case 'number':
            if (Number.isInteger(value)) {
                return { type: 'integer', value };
            }
            return { type: 'numeric', value };
        case 'string': return { type: 'string', value };
        case 'boolean': return { type: 'boolean', value };
        case 'bigint': return { type: 'integer', value: Number(value) };
        case 'object': {
            if (value instanceof Date) {
                return { type: 'string', value: moment(value).toISOString() };
            }
            return { type: 'string', value: JSON.stringify(value) };
        }
        default: throw new Error('Unsupported parameter type');
    }
}


class Bound implements IBoundQuery {
    constructor(private parent: PreparedQuery, private results: StatementResult[], private lastSelect: SelectExec | null) {
    }


    executeAll(): QueryResult {
        const f = this.results[this.results.length - 1];
        this.commit();
        return f.result
    }

    *iterate(): IterableIterator<QueryResult> {
        for (const res of this.results) {
            yield res.result;
        }
        this.commit();
    }

    private commit() {
        // commit transaction
        this.results[this.results.length - 1].state.fullCommit();
        if (this.lastSelect) {
            this.parent.schema.lastSelect = this.lastSelect.selection;
        }
        this.parent.executed?.();
    }


}
