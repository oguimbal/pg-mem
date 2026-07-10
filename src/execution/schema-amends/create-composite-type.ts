import { _ISchema, _Transaction, _IStatementExecutor, _IStatement, StatementResult } from '../../interfaces-private';
import { CreateCompositeType as CreateCompositeTypeStatement } from 'pgsql-ast-parser';
import { ExecHelper } from '../exec-utils';
import { RecordCol } from '../../datatypes';
import { CompositeType } from '../../datatypes/t-composite';

export class CreateCompositeType extends ExecHelper implements _IStatementExecutor {
    private onSchema: _ISchema;
    private name: string;
    private columns: RecordCol[];

    constructor({ schema }: _IStatement, p: CreateCompositeTypeStatement) {
        super(p);
        this.onSchema = schema.getThisOrSiblingFor(p.name);
        this.name = p.name.name;
        this.columns = p.attributes.map(a => ({
            name: a.name.name,
            type: schema.getType(a.dataType),
        }));
    }

    execute(t: _Transaction): StatementResult {
        t = t.fullCommit();
        new CompositeType(this.onSchema, this.name, this.columns).install();
        t = t.fork();
        return this.noData(t, 'CREATE TYPE');
    }
}
