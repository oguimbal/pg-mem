import { _ISchema, _Transaction, _IStatementExecutor, _IStatement, StatementResult } from '../../interfaces-private.ts';
import { CreateCompositeType as CreateCompositeTypeStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { RecordCol } from '../../datatypes/index.ts';
import { CompositeType } from '../../datatypes/t-composite.ts';

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
