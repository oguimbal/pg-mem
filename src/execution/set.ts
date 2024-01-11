import { _IStatementExecutor, _Transaction, StatementResult, GLOBAL_VARS, QueryError } from '../interfaces-private';
import { SetGlobalStatement, SetTimezone, SetNames } from 'pgsql-ast-parser';
import { ignore } from '../utils';
import { ExecHelper } from './exec-utils';

export class SetExecutor extends ExecHelper implements _IStatementExecutor {

    constructor(private p: SetGlobalStatement | SetTimezone | SetNames) {
        super(p);
        // todo handle set statements timezone ?
        // They are just ignored as of today (in order to handle pg_dump exports)
        ignore(p);
    }

    execute(t: _Transaction): StatementResult {
        const p = this.p;
        if (p.type === 'set' && p.set.type === 'value') {
            t.set(GLOBAL_VARS, t.getMap(GLOBAL_VARS)
                .set(p.variable.name, p.set.value));
        }
        return this.noData(t, 'SET');
    }
}
