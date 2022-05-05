import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq } from '../../interfaces-private.ts';
import { DropSequenceStatement } from 'https://deno.land/x/pgsql_ast_parser@10.0.5/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore } from '../../utils.ts';

export class DropSequence extends ExecHelper implements _IStatementExecutor {
    private seq: _ISequence | null;

    constructor({ schema }: _IStatement, statement: DropSequenceStatement) {
        super(statement);

        this.seq = asSeq(schema.getObject(statement.name, {
            nullIfNotFound: statement.ifExists,
        }));
        if (!this.seq) {
            ignore(statement);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();

        // drop the sequence
        this.seq?.drop(t);

        // new implicit transaction
        t = t.fork();

        return this.noData(t, 'DROP');
    }
}
