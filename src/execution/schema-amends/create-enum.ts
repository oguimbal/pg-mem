import { _Transaction, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor } from '../../interfaces-private';
import { CreateEnumType } from 'pgsql-ast-parser';
import { ExecHelper } from '../exec-utils';

export class CreateEnum extends ExecHelper implements _IStatementExecutor {
    private onSchema: _ISchema;
    private values: string[];
    private name: string;

    constructor({ schema }: _IStatement, st: CreateEnumType) {
        super(st);
        this.onSchema = schema.getThisOrSiblingFor(st.name);
        this.values = st.values.map(x => x.value);
        this.name = st.name.name;
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        // register enum
        this.onSchema
            .registerEnum(this.name, this.values);

        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}
