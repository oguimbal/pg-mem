import { _IStatementExecutor, _Transaction, StatementResult, _IStatement, CompiledFunction, _ISchema } from '../../interfaces-private.ts';
import { DoStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ExecHelper } from '../../execution/exec-utils.ts';
import { pushExecutionCtx } from '../../utils.ts';

export class DoStatementExec extends ExecHelper implements _IStatementExecutor {
    private compiled: CompiledFunction;
    private schema: _ISchema;

    constructor({ schema }: _IStatement, st: DoStatement) {
        super(st);
        this.schema = schema;
        const lang = schema.db.getLanguage(st.language?.name ?? 'plpgsql');
        this.compiled = lang({
            args: [],
            code: st.code,
            schema: schema,
        });
    }

    execute(t: _Transaction): StatementResult {
        // embedded DDL in the body can fork/commit the transaction; capture the
        // final handle so the outer statement commits the right one.
        let finalT = t;
        pushExecutionCtx({
            schema: this.schema,
            transaction: t,
            onTransaction: nt => { finalT = nt; },
        }, () => {
            this.compiled();
        });
        return this.noData(finalT, 'DO');
    }
}
