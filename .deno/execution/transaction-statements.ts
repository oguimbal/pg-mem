import { _IStatementExecutor, _Transaction, StatementResult } from '../interfaces-private.ts';
import { ExecHelper } from './exec-utils.ts';
import { CommitStatement, RollbackStatement, StartTransactionStatement, BeginStatement, SavepointStatement, ReleaseSavepointStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ignore } from '../utils.ts';

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
    constructor(private stmt: RollbackStatement) {
        super(stmt);
    }

    execute(t: _Transaction): StatementResult {
        // "ROLLBACK TO [SAVEPOINT] x" rewinds to a savepoint instead of ending the tx
        if (this.stmt.to) {
            t.rollbackTo(this.stmt.to.name);
            return this.noData(t, 'ROLLBACK');
        }
        t = t.rollback();
        return this.noData(t, 'ROLLBACK');
    }
}

export class SavepointExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(private stmt: SavepointStatement) {
        super(stmt);
    }

    execute(t: _Transaction): StatementResult {
        t.savepoint(this.stmt.name.name);
        return this.noData(t, 'SAVEPOINT');
    }
}

export class ReleaseSavepointExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(private stmt: ReleaseSavepointStatement) {
        super(stmt);
    }

    execute(t: _Transaction): StatementResult {
        t.release(this.stmt.name.name);
        return this.noData(t, 'RELEASE');
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
