import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq, asIndex, _INamedIndex, _ITable, asTable } from '../../interfaces-private';
import { DropTableStatement } from 'pgsql-ast-parser';
import { ExecHelper } from '../exec-utils';
import { ignore } from '../../utils';

export class DropTable extends ExecHelper implements _IStatementExecutor {
    private table: _ITable | null;
    private cascade: boolean;


    constructor({ schema }: _IStatement, statement: DropTableStatement) {
        super(statement);

        this.table = asTable(schema.getObject(statement.name, {
            nullIfNotFound: statement.ifExists,
        }));

        this.cascade = statement.cascade === 'cascade';

        if (!this.table) {
            ignore(statement);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because it does not support further rollbacks)
        t = t.fullCommit();

        // drop table
        this.table?.drop(t, this.cascade);

        // new implicit transaction
        t = t.fork();

        return this.noData(t, 'DROP');
    }
}
