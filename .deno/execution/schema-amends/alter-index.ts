import { _ISchema, _Transaction, _IStatementExecutor, _IStatement, asIndex, NotSupported } from '../../interfaces-private.ts';
import { AlterIndexStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore } from '../../utils.ts';

export class AlterIndex extends ExecHelper implements _IStatementExecutor {
    private schema: _ISchema;

    constructor({ schema }: _IStatement, private p: AlterIndexStatement) {
        super(p);
        this.schema = schema;
    }

    execute(t: _Transaction) {
        // commit pending data before making schema changes (no rollback support)
        t = t.fullCommit();

        const idx = asIndex(this.schema.getObject(this.p.index, {
            nullIfNotFound: this.p.ifExists,
        }));
        if (idx) {
            const change = this.p.change;
            switch (change.type) {
                case 'rename':
                    idx.onTable.renameIndex(idx.name, change.to.name);
                    break;
                case 'set tablespace':
                    // tablespaces are meaningless in-memory
                    ignore(change);
                    break;
                default:
                    throw NotSupported.never(change);
            }
        } else {
            ignore(this.p);
        }

        t = t.fork();
        return this.noData(t, 'ALTER INDEX');
    }
}
