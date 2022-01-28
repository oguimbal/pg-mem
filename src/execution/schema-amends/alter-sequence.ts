import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq } from '../../interfaces-private';
import { AlterSequenceStatement } from 'pgsql-ast-parser';
import { resultNoData } from '../exec-utils';
import { ignore } from 'utils';

export class AlterSequence implements _IStatementExecutor {
    private seq: _ISequence | null;


    constructor({ schema }: _IStatement, private statement: AlterSequenceStatement) {

        this.seq = asSeq(schema.getObject(statement.name, {
            nullIfNotFound: statement.ifExists,
        }));
        if (!this.seq) {
            ignore(this.statement);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();

        // alter the sequence
        this.seq?.alter(t, this.statement.change);

        // new implicit transaction
        t = t.fork();

        return resultNoData('ALTER', this.statement, t, this.seq === null);
    }
}
