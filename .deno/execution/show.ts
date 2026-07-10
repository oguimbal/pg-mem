import { _IStatementExecutor, _Transaction, StatementResult, GLOBAL_VARS, QueryError } from '../interfaces-private.ts';
import { ShowStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { locOf } from './exec-utils.ts';

const CANONICAL_GUC_CASING: { [lower: string]: string } = {
    timezone: 'TimeZone',
    datestyle: 'DateStyle',
    intervalstyle: 'IntervalStyle',
};

export class ShowExecutor implements _IStatementExecutor {
    constructor(private statement: ShowStatement) { }

    execute(t: _Transaction): StatementResult {
        const p = this.statement;
        const got = t.getMap(GLOBAL_VARS);
        if (!got.has(p.variable.name)) {
            throw new QueryError(`unrecognized configuration parameter "${p.variable.name}"`);
        }
        // pg echoes the canonical GUC casing in the column name
        const colName = CANONICAL_GUC_CASING[p.variable.name] ?? p.variable.name;
        return {
            state: t,
            result: {
                rows: [{ [colName]: got.get(p.variable.name) }],
                rowCount: 1,
                command: 'SHOW',
                fields: [],
                location: locOf(p),
            },
        }
    }
}
