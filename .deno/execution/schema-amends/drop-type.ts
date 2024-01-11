import { _ISchema, _Transaction, _ISequence, _IStatementExecutor, _IStatement, asSeq, asType, _IType } from '../../interfaces-private.ts';
import { DropStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore, notNil } from '../../utils.ts';

export class DropType extends ExecHelper implements _IStatementExecutor {
    private types: _IType[];

    constructor({ schema }: _IStatement, statement: DropStatement) {
        super(statement);

        this.types = notNil(statement.names.map(x => asType(schema.getObject(x, {
            nullIfNotFound: statement.ifExists,
        }))));
        if (!this.types.length) {
            ignore(statement);
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();

        // drop the sequence
        for (const seq of this.types) {
            seq.drop(t);
        }

        // new implicit transaction
        t = t.fork();

        return this.noData(t, 'DROP');
    }
}
