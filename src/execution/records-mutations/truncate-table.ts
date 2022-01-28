import { _ISchema, _Transaction, SchemaField, NotSupported, _ITable, _IStatementExecutor, asTable, StatementResult, _IStatement } from '../../interfaces-private';
import { TruncateTableStatement } from 'pgsql-ast-parser';
import { resultNoData } from '../exec-utils';

export class TruncateTable implements _IStatementExecutor {
    private table: _ITable;
    constructor({ schema }: _IStatement, private statement: TruncateTableStatement) {
        if (this.statement.tables.length !== 1) {
            throw new NotSupported('Multiple truncations');
        }
        this.table = asTable(schema.getObject(this.statement.tables[0]));
    }

    execute(t: _Transaction): StatementResult {
        this.table.truncate(t);
        return resultNoData('TRUNCATE', this.statement, t);
    }
}
