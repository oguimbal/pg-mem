import { _IStatementExecutor, _Transaction, StatementResult, _IStatement, CompiledFunction } from '../../interfaces-private';
import { DoStatement } from 'pgsql-ast-parser';
import { resultNoData } from '../../execution/exec-utils';

export class DoStatementExec implements _IStatementExecutor {
    private compiled: CompiledFunction;

    constructor({ schema }: _IStatement, private st: DoStatement) {
        const lang = schema.db.getLanguage(st.language?.name ?? 'plpgsql');
        this.compiled = lang({
            args: [],
            code: st.code,
            schema: schema,
        });
    }

    execute(t: _Transaction): StatementResult {
        this.compiled();
        return resultNoData('DO', this.st, t);
    }
}
