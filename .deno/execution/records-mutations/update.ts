import { _ITable, _Transaction, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private.ts';
import { UpdateStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { MutationDataSourceBase, Setter, createSetter } from './mutation-base.ts';
import { buildCtx } from '../../parser/context.ts';
import { buildSelect } from '../select.ts';
import { Selection } from '../../transforms/selection.ts';
import { JoinSelection } from '../../transforms/join.ts';

export class Update extends MutationDataSourceBase<any> {

    private setter: Setter;
    private fetchObjectToUpdate?: ((x: any) => any);

    constructor(ast: UpdateStatement) {
        const { schema } = buildCtx();
        const into = asTable(schema.getObject(ast.table));
        let mutatedSel: _ISelection;
        let fetchObjectToUpdate: ((x: any) => any) | undefined;
        if (ast.from) {

            //  => UPDATE-FROM-SELECT

            // build a join that selects the full record to update,
            // based on the data from the original selection
            mutatedSel = buildSelect({
                type: 'select',
                // join from:
                from: [
                    ast.from,
                    {
                        type: 'table',
                        name: ast.table,
                        join: {
                            type: 'INNER JOIN',
                            on: ast.where,
                        }
                    }],
                // // select the whole updated record
                columns: [{
                    expr: {
                        type: 'ref',
                        table: ast.table,
                        name: '*',
                    }
                }]
            });

            // this should have built a selection on a join statement
            if (!(mutatedSel instanceof Selection)) {
                throw new Error('Invalid select-from statement');
            }
            mutatedSel = mutatedSel.base;
            if (!(mutatedSel instanceof JoinSelection)) {
                // should not happen
                throw new Error('Invalid select-from statement');
            }
            // use hack to get the full joined source in the selection
            fetchObjectToUpdate = x => x['>joined'];
        } else {

            //  => REGULAR UPDATE
            mutatedSel = into
                .selection
                .filter(ast.where);
        }


        super(into, mutatedSel, ast);

        this.setter = createSetter(this.table, this.mutatedSel, ast.sets);
        this.fetchObjectToUpdate = fetchObjectToUpdate;

    }

    protected performMutation(t: _Transaction): any[] {
        // perform update
        const rows: any[] = [];
        for (const i of this.mutatedSel.enumerate(t)) {
            const data = this.fetchObjectToUpdate
                ? this.fetchObjectToUpdate(i)
                : i;
            this.setter(t, data, i);
            rows.push(this.table.update(t, data));
        }
        return rows;
    }
}
