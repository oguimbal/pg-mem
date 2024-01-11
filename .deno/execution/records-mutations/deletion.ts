import { _ITable, _Transaction, IValue, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private.ts';
import { DeleteStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { MutationDataSourceBase } from './mutation-base.ts';
import { buildCtx } from '../../parser/context.ts';

export class Deletion extends MutationDataSourceBase<any> {


    constructor(ast: DeleteStatement) {
        const { schema } = buildCtx();
        const table = asTable(schema.getObject(ast.from));
        const mutatedSel = table
            .selection
            .filter(ast.where);

        super(table, mutatedSel, ast);
    }

    protected performMutation(t: _Transaction): any[] {
        // perform deletion
        const rows = [];
        for (const item of this.mutatedSel.enumerate(t)) {
            this.table.delete(t, item);
            rows.push(item);
        }
        return rows;
    }
}
