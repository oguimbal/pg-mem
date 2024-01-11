import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq } from '../../interfaces-private.ts';
import { DropStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore, notNil } from '../../utils.ts';

export class DropSequence extends ExecHelper implements _IStatementExecutor {
    private seqs: _ISequence[];

    constructor({ schema }: _IStatement, statement: DropStatement) {
        super(statement);

        this.seqs = notNil(statement.names.map(x => asSeq(schema.getObject(x, {
            nullIfNotFound: statement.ifExists,
        }))));
        if (!this.seqs.length) {
            ignore(statement);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();

        // drop the sequence
        for (const seq of this.seqs) {
            seq.drop(t);
        }

        // new implicit transaction
        t = t.fork();

        return this.noData(t, 'DROP');
    }
}
