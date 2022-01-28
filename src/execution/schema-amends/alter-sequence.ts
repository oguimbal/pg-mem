import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq } from '../../interfaces-private';
import { AlterSequenceStatement } from 'pgsql-ast-parser';
import { ExecHelper } from '../exec-utils';
import { ignore } from '../../utils';

export class AlterSequence extends ExecHelper implements _IStatementExecutor {
    private seq: _ISequence | null;


    constructor({ schema }: _IStatement, private p: AlterSequenceStatement) {
        super(p);

        this.seq = asSeq(schema.getObject(p.name, {
            nullIfNotFound: p.ifExists,
        }));
        if (!this.seq) {
            ignore(this.p);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();

        // alter the sequence
        this.seq?.alter(t, this.p.change);

        // new implicit transaction
        t = t.fork();

        return this.noData(t, 'ALTER');
    }
}
