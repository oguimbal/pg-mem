import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq, asIndex, _INamedIndex, _ITable, asTable } from '../../interfaces-private';
import { DropTableStatement } from 'pgsql-ast-parser';
import { resultNoData } from '../exec-utils';
import { ignore } from 'utils';

export class DropTable implements _IStatementExecutor {
    private table: _ITable | null;


    constructor({ schema }: _IStatement, private statement: DropTableStatement) {

        this.table = asTable(schema.getObject(this.statement.name, {
            nullIfNotFound: this.statement.ifExists,
        }));

        if (!this.table) {
            ignore(statement);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because it does not support further rollbacks)
        t = t.fullCommit();

        // drop table
        this.table?.drop(t);

        // new implicit transaction
        t = t.fork();

        return resultNoData('DROP', this.statement, t, this.table === null);
    }
}
