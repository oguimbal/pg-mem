import { _IStatementExecutor, _Transaction, StatementResult, GLOBAL_VARS, QueryError } from '../interfaces-private';
import { SetGlobalStatement, SetTimezone } from 'pgsql-ast-parser';
import { resultNoData } from './exec-utils';
import { ignore } from 'utils';

export class SetExecutor implements _IStatementExecutor {

    constructor(private p: SetGlobalStatement | SetTimezone) {
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
        return resultNoData('SET', p, t);
    }
}
