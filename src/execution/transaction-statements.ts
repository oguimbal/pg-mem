import { _IStatementExecutor, _Transaction, StatementResult } from '../interfaces-private';
import { ExecHelper } from './exec-utils';
import { CommitStatement, RollbackStatement, StartTransactionStatement, BeginStatement } from 'pgsql-ast-parser';
import { ignore } from '../utils';

export class CommitExecutor extends ExecHelper implements _IStatementExecutor {

    constructor(statement: CommitStatement) {
        super(statement)
    }

    execute(t: _Transaction): StatementResult {
        t = t.commit();
        // recreate an implicit transaction if we're at root
        // (I can see how its usfull, but this is dubious...)
        if (!t.isChild) {
            t = t.fork();
        }
        return this.noData(t, 'COMMIT');
    }

}

export class RollbackExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(statement: RollbackStatement) {
        super(statement);
        ignore(statement);
    }

    execute(t: _Transaction): StatementResult {
        t = t.rollback();
        return this.noData(t, 'ROLLBACK');
    }
}


export class BeginStatementExec extends ExecHelper implements _IStatementExecutor {
    constructor(statement: BeginStatement | StartTransactionStatement) {
        super(statement);
        ignore(statement);
    }

    execute(t: _Transaction): StatementResult {
        t = t.fork();
        return this.noData(t, 'BEGIN');
    }
}
