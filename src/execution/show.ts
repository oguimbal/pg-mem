import { _IStatementExecutor, _Transaction, StatementResult, GLOBAL_VARS, QueryError } from '../interfaces-private';
import { ShowStatement } from 'pgsql-ast-parser';
import { locOf } from './exec-utils';

export class ShowExecutor implements _IStatementExecutor {
    constructor(private statement: ShowStatement) { }

    execute(t: _Transaction): StatementResult {
        const p = this.statement;
        const got = t.getMap(GLOBAL_VARS);
        if (!got.has(p.variable.name)) {
            throw new QueryError(`unrecognized configuration parameter "${p.variable.name}"`);
        }
        return {
            state: t,
            result: {
                rows: [{ [p.variable.name]: got.get(p.variable.name) }],
                rowCount: 1,
                command: 'SHOW',
                fields: [],
                location: locOf(p),
            },
        }
    }
}
