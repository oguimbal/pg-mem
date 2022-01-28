import { _Transaction, asTable, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor, asView, _IView, QueryError } from '../../interfaces-private';
import { CreateMaterializedViewStatement } from 'pgsql-ast-parser';
import { resultNoData } from '../exec-utils';
import { View } from '../../schema/view';
import { buildSelect } from '../select';

export class CreateMaterializedView implements _IStatementExecutor {
    private schema: _ISchema;
    private toRegister?: View;


    constructor(st: _IStatement, private p: CreateMaterializedViewStatement) {
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
            return resultNoData('CREATE', this.p, t, true);
        }

        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        // view creation
        this.toRegister.register();

        // new implicit transaction
        t = t.fork();
        return resultNoData('CREATE', this.p, t);
    }
}