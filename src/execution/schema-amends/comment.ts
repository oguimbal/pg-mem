import { _Transaction, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor, QueryError } from '../../interfaces-private';
import { CommentStatement, CreateEnumType } from 'pgsql-ast-parser';
import { ExecHelper } from '../exec-utils';
import { ignore, qnameToStr } from '../../utils';
import { MemoryTable } from '../../table';
import { ColRef } from '../../column';

export class Comment extends ExecHelper implements _IStatementExecutor {
    private schema: _ISchema;
    constructor({ schema }: _IStatement, private p: CommentStatement) {
        super(p);
        this.schema = schema;
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        const on = this.p.on;
        switch (on.type) {
            case 'table': {
                const table = this.schema.getObject(on.name);
                if (!(table instanceof MemoryTable)) {
                    throw new QueryError(`relation ${qnameToStr(on.name)} is not a writeable table`);
                }
                table.comment = this.p.comment;
                break;
            }
            case 'column': {
                let s = this.schema;
                if (on.column.schema) {
                    s = s.db.getSchema(on.column.schema);
                }
                const table = s.getTable(on.column.table);
                const col = table.getColumnRef(on.column.column);
                if (!(col instanceof ColRef)) {
                    throw new QueryError(`column ${on.column} is not a writeable column`);
                }
                col.comment = this.p.comment;
                break;
            }
            default:
                ignore(this.p);
                break;
        }


        return this.noData(t, 'COMMENT');
    }
}