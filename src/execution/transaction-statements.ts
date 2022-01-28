import { _IStatementExecutor, _Transaction, StatementResult } from '../interfaces-private';
import { resultNoData } from './exec-utils';
import { CommitStatement, RollbackStatement, StartTransactionStatement, BeginStatement } from 'pgsql-ast-parser';
import { ignore } from '../utils';

export class CommitExecutor implements _IStatementExecutor {

    constructor(private statement: CommitStatement) { }

    execute(t: _Transaction): StatementResult {
        t = t.commit();
        // recreate an implicit transaction if we're at root
        // (I can see how its usfull, but this is dubious...)
        if (!t.isChild) {
            t = t.fork();
        }
        return resultNoData('COMMIT', this.statement, t);
    }

}

export class RollbackExecutor implements _IStatementExecutor {
    constructor(private statement: RollbackStatement) {
        ignore(statement);
    }

    execute(t: _Transaction): StatementResult {
        t = t.rollback();
        return resultNoData('ROLLBACK', this.statement, t);
    }
}


export class BeginStatementExec implements _IStatementExecutor {
    constructor(private statement: BeginStatement | StartTransactionStatement) {
        ignore(statement);
    }

    execute(t: _Transaction): StatementResult {
        t = t.fork();
        return resultNoData('BEGIN', this.statement, t);
    }
}
