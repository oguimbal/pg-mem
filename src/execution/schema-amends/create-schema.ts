import { _Transaction, asTable, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor, QueryError } from '../../interfaces-private';
import { CreateSchemaStatement } from 'pgsql-ast-parser';
import { resultNoData } from '../exec-utils';
import { ignore } from '../../utils';

export class CreateSchema implements _IStatementExecutor {
    private toCreate?: string;

    constructor(private st: _IStatement, private p: CreateSchemaStatement) {
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
        return resultNoData('CREATE', this.p, t);
    }
}
