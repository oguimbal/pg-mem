import { _ISchema, _Transaction, SchemaField, NotSupported, _ITable, _IStatementExecutor, asTable, StatementResult, _IStatement } from '../../interfaces-private';
import { TruncateTableStatement } from 'pgsql-ast-parser';
import { ExecHelper } from '../exec-utils';
import { buildCtx } from '../../parser/context';

export class TruncateTable extends ExecHelper implements _IStatementExecutor {
    private table: _ITable;
    constructor(statement: TruncateTableStatement) {
        super(statement);
        if (statement.tables.length !== 1) {
            throw new NotSupported('Multiple truncations');
        }
        const { schema } = buildCtx();
        this.table = asTable(schema.getObject(statement.tables[0]));
    }

    execute(t: _Transaction): StatementResult {
        this.table.truncate(t);
        return this.noData(t, 'TRUNCATE');
    }
}
