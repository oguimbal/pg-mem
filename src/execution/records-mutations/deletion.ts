import { _ITable, _Transaction, IValue, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private';
import { applyReadRls } from '../rls-enforce';
import { fireRowTriggers, fireStatementTriggers, SKIP_ROW } from '../triggers';
import { DeleteStatement } from 'pgsql-ast-parser';
import { MutationDataSourceBase } from './mutation-base';
import { buildCtx } from '../../parser/context';

export class Deletion extends MutationDataSourceBase {


    constructor(ast: DeleteStatement) {
        const { schema } = buildCtx();
        const table = asTable(schema.getObject(ast.from));
        // row-level security: DELETE can only affect rows visible via DELETE policies
        const mutatedSel = applyReadRls(table, table.selection, 'delete')
            .filter(ast.where);

        super(table, mutatedSel, ast);
    }

    protected performMutation(t: _Transaction): any[] {
        // perform deletion
        const rows = [];
        const hasTriggers = this.table.triggers.triggers.length > 0;
        if (hasTriggers) { fireStatementTriggers(this.table, 'before', 'delete', t); }
        for (const item of this.mutatedSel.enumerate(t)) {
            // BEFORE DELETE row triggers may (returning null) skip the deletion
            if (this.table.triggers.triggers.length) {
                if (fireRowTriggers(this.table, 'before', 'delete', null, item, t) === SKIP_ROW) {
                    continue;
                }
            }
            this.table.delete(t, item);
            if (this.table.triggers.triggers.length) {
                fireRowTriggers(this.table, 'after', 'delete', null, item, t);
            }
            rows.push(item);
        }
        if (hasTriggers) { fireStatementTriggers(this.table, 'after', 'delete', t); }
        return rows;
    }
}
