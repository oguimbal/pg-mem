import { Statement } from 'pgsql-ast-parser';
import { _IDb, _ISchema, _IStatementExecutor, _Transaction, IBoundQuery, IPreparedQuery, nil, QueryResult, StatementResult } from '../interfaces-private';
import { StatementExec } from '../execution/statement-exec';
import { SelectExec } from '../execution/select';

export class PreparedQuery implements IPreparedQuery {
    private next?: PreparedQuery;
    private statementExec: StatementExec;
    private compiled: _IStatementExecutor;

    executed?: () => void;
    failed?: (e: any) => void;

    constructor(private schema: _ISchema, query: Statement[], singleSql: string | nil) {

        const thisQuery = query.shift()!;

        this.statementExec = new StatementExec(schema, thisQuery, singleSql);
        this.compiled = this.statementExec.compile();


        if (query.length) {
            this.next = new PreparedQuery(schema, query, null);
        }
    }

    bind(...args: any[]): Bound {
        const nextBound = this.next?.bind(...args);

        const bound = new Bound(this.schema, this.compiled, this.statementExec, nextBound);
        return bound;
    }

    // get last select for debug purposes
    lastSelect(): SelectExec | null {
        const thisSelect = this.compiled instanceof SelectExec
            ? this.compiled
            : null;
        return this.next?.lastSelect() ?? thisSelect;
    }
}


class Bound implements IBoundQuery {
    constructor(
        private schema: _ISchema,
        private executor: _IStatementExecutor,
        private statementExec: StatementExec,
        private next?: Bound) {
    }


    executeAll(): QueryResult {
        let last: QueryResult;
        for (const res of this.iterate()) {
            last = res;
        }
        return last!;
    }

    *iterate(): IterableIterator<QueryResult> {

        // Start an implicit transaction
        //  (to avoid messing global data if an operation fails mid-write)
        const t = this.schema.db.data.fork();

        let last: StatementResult | undefined;
        for (const res of this.execute(t)) {
            last = res;
        }

        // implicit final commit
        last?.state.fullCommit();
    }



    *execute(t: _Transaction): IterableIterator<StatementResult> {

        // store last select for debug purposes
        if (this.executor instanceof SelectExec) {
            this.schema.lastSelect = this.executor.selection;
        }

        // Execute statement
        const r = this.statementExec.executeStatement(t);
        yield r;

        // Execute next statement
        if (this.next) {
            return yield* this.next.execute(r.state);
        }
    }
}
