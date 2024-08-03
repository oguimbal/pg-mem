import { DataTypeDef, Expr, Statement } from 'pgsql-ast-parser';
import { _IDb, _ISchema, _IStatementExecutor, _Transaction, IBoundQuery, IPreparedQuery, nil, QueryError, QueryResult, StatementResult } from '../interfaces-private';
import { StatementExec } from '../execution/statement-exec';
import { SelectExec } from '../execution/select';
import { astMapper } from 'pgsql-ast-parser';
import { nullIsh } from '../utils';
import moment from 'moment';

export class PreparedQuery implements IPreparedQuery {

    executed?: () => void;
    failed?: (e: any) => void;

    constructor(readonly schema: _ISchema, private query: Statement[], private singleSql: string | nil) {
    }

    bind(...args: any[]): Bound {

        // Start an implicit transaction
        //  (to avoid messing global data if an operation fails mid-write)
        let t = this.schema.db.data.fork();

        // bind parameters
        let query = this.query;
        if (args.length) {
            const mapper = astMapper(map => ({
                parameter: p => {
                    const [, istr] = /^\$(\d+)$/.exec(p.name) ?? [];
                    if (!istr) {
                        throw new QueryError('Invalid parameter name');
                    }
                    const i = parseInt(istr, 10) - 1;
                    if (i >= args.length) {
                        throw new QueryError('Parameter out of range');
                    }
                    const value = dataToLiteral(args[i]);
                    return value;
                }
            }));
            query = this.query.map(stmt => mapper.statement(stmt)!);
        }



        let results: StatementResult[] = [];
        let lastSelect: SelectExec | null = null;
        for (const s of query) {
            const statementExec = new StatementExec(this.schema, s, this.singleSql);
            const compiled = statementExec.compile();
            // store last select for debug purposes
            if (compiled instanceof SelectExec) {
                lastSelect = compiled;
            }

            // Execute statement
            const r = statementExec.executeStatement(t);
            results.push(r);
            t = r.state;
        }

        return new Bound(this, results, lastSelect);
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
