import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq, asIndex, _INamedIndex } from '../../interfaces-private.ts';
import { DropStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore, notNil } from '../../utils.ts';

export class DropIndex extends ExecHelper implements _IStatementExecutor {
    private idx: _INamedIndex<any>[];


    constructor({ schema }: _IStatement, statement: DropStatement) {
        super(statement);

        this.idx = notNil(statement.names.map(x => asIndex(schema.getObject(x, {
            nullIfNotFound: statement.ifExists,
        }))));

        if (this.idx.length) {
            ignore(statement.concurrently);
        } else {
            ignore(statement);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();

        // alter the sequence
        for (const idx of this.idx) {
            idx.onTable.dropIndex(t, idx.name);
        }

        // new implicit transaction
        t = t.fork();

        return this.noData(t, 'DROP');
    }
}
