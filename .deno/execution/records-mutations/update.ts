import { _ITable, _Transaction, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private.ts';
import { applyReadRls, checkWriteRls } from '../rls-enforce.ts';
import { fireRowTriggers, fireStatementTriggers, SKIP_ROW } from '../triggers.ts';
import { UpdateStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { MutationDataSourceBase, Setter, createSetter } from './mutation-base.ts';
import { buildCtx } from '../../parser/context.ts';
import { buildSelect } from '../select.ts';
import { Selection } from '../../transforms/selection.ts';
import { JoinSelection } from '../../transforms/join.ts';
import { deepCloneSimple } from '../../utils.ts';

export class Update extends MutationDataSourceBase {

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
            // row-level security: UPDATE can only affect rows visible via UPDATE policies
            mutatedSel = applyReadRls(into, into.selection, 'update')
                .filter(ast.where);
        }


        super(into, mutatedSel, ast);

        this.setter = createSetter(this.table, this.mutatedSel, ast.sets);
        this.fetchObjectToUpdate = fetchObjectToUpdate;

    }

    protected performMutation(t: _Transaction): any[] {
        // perform update
        const rows: any[] = [];
        const hasTriggers = this.table.triggers.triggers.length > 0;
        if (hasTriggers) { fireStatementTriggers(this.table, 'before', 'update', t); }
        for (const i of this.mutatedSel.enumerate(t)) {
            const data = deepCloneSimple(this.fetchObjectToUpdate
                ? this.fetchObjectToUpdate(i)
                : i);
            this.setter(t, data, i);
            // BEFORE UPDATE row triggers may modify the new row or (null) skip it
            let neu = data;
            if (this.table.triggers.triggers.length) {
                neu = fireRowTriggers(this.table, 'before', 'update', data, i, t);
                if (neu === SKIP_ROW) {
                    continue;
                }
            }
            // row-level security: the updated row must satisfy WITH CHECK
            checkWriteRls(this.table, 'update', neu, t);
            const updated = this.table.update(t, neu);
            if (this.table.triggers.triggers.length) {
                fireRowTriggers(this.table, 'after', 'update', updated, i, t);
            }
            rows.push(updated);
        }
        if (hasTriggers) { fireStatementTriggers(this.table, 'after', 'update', t); }
        return rows;
    }
}
