import { _Transaction, asTable, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor, QueryError } from '../../interfaces-private.ts';
import { CreateSchemaStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore } from '../../utils.ts';

export class CreateSchema extends ExecHelper implements _IStatementExecutor {
    private toCreate?: string;

    constructor(private st: _IStatement, p: CreateSchemaStatement) {
        super(p);
        const sch = this.st.schema.db.getSchema(p.name.name, true);
        if (!p.ifNotExists && sch) {
            throw new QueryError('schema already exists! ' + p.name);
        }
        if (sch) {
            ignore(p);
        } else {
            this.toCreate = p.name.name;
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        // create schema
        if (this.toCreate) {
            this.st.schema.db.createSchema(this.toCreate);
        }

        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}
