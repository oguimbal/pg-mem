import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq } from '../../interfaces-private.ts';
import { AlterSequenceStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore } from '../../utils.ts';

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
