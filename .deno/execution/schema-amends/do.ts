import { _IStatementExecutor, _Transaction, StatementResult, _IStatement, CompiledFunction } from '../../interfaces-private.ts';
import { DoStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../../execution/exec-utils.ts';

export class DoStatementExec extends ExecHelper implements _IStatementExecutor {
    private compiled: CompiledFunction;

    constructor({ schema }: _IStatement, st: DoStatement) {
        super(st);
        const lang = schema.db.getLanguage(st.language?.name ?? 'plpgsql');
        this.compiled = lang({
            args: [],
            code: st.code,
            schema: schema,
        });
    }

    execute(t: _Transaction): StatementResult {
        this.compiled();
        return this.noData(t, 'DO');
    }
}
