import { _ISchema, _Transaction, _IStatementExecutor, _IStatement, IValue, QueryError, StatementResult } from '../interfaces-private.ts';
import { ExecuteStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ExecHelper } from './exec-utils.ts';
import { buildValue } from '../parser/expression-builder.ts';

/** SQL-level `EXECUTE <name>(args)` — runs a previously PREPAREd statement. */
export class ExecutePrepared extends ExecHelper implements _IStatementExecutor {
    private schema: _ISchema;
    private argValues: IValue[];

    constructor({ schema }: _IStatement, private p: ExecuteStatement) {
        super(p);
        this.schema = schema;
        // arg expressions are compiled here (build context is available during compile)
        this.argValues = (p.args ?? []).map(a => buildValue(a));
    }

    execute(t: _Transaction): StatementResult {
        const runner = this.schema.db.preparedStatements.get(this.p.name.name);
        if (!runner) {
            throw new QueryError(`prepared statement "${this.p.name.name}" does not exist`, '26000');
        }
        const args = this.argValues.map(v => v.get(null, t));
        return runner(args, t);
    }
}
