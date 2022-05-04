import { _ITable, _Transaction, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private.ts';
import { UpdateStatement } from 'https://deno.land/x/pgsql_ast_parser@10.0.3/mod.ts';
import { MutationDataSourceBase, Setter, createSetter } from './mutation-base.ts';
import { buildCtx } from '../../parser/context.ts';

export class Update extends MutationDataSourceBase<any> {

    private setter: Setter;

    constructor(ast: UpdateStatement) {
        const { schema } = buildCtx();
        const into = asTable(schema.getObject(ast.table));
        const mutatedSel = into
            .selection
            .filter(ast.where);

        super(into, mutatedSel, ast);

        this.setter = createSetter(this.table, this.mutatedSel, ast.sets);

    }

    protected performMutation(t: _Transaction): any[] {
        // perform update
        const rows: any[] = [];
        for (const i of this.mutatedSel.enumerate(t)) {
            this.setter(t, i, i);
            rows.push(this.table.update(t, i));
        }
        return rows;
    }
}
