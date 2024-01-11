import { _Transaction, asTable, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor, asView, _IView, QueryError } from '../../interfaces-private.ts';
import { CreateMaterializedViewStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { View } from '../../schema/view.ts';
import { buildSelect } from '../select.ts';

export class CreateMaterializedView extends ExecHelper implements _IStatementExecutor {
    private schema: _ISchema;
    private toRegister?: View;


    constructor(st: _IStatement, p: CreateMaterializedViewStatement) {
        super(p);
        this.schema = st.schema.getThisOrSiblingFor(p.name);
        // check existence
        const existing = this.schema.getObject(p.name, { nullIfNotFound: true });
        if (existing) {
            if (p.ifNotExists) {
                return;
            }
            throw new QueryError(`Name already exists: ${p.name.name}`);
        }

        const view = buildSelect(p.query);

        // hack: materialized views are implemented as simple views :/  (todo ?)
        this.toRegister = new View(this.schema, p.name.name, view);
    }


    execute(t: _Transaction) {
        if (!this.toRegister) {
            return this.noData(t, 'CREATE');
        }

        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        // view creation
        this.toRegister.register();

        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}