import { _ITable, _Transaction, IValue, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private.ts';
import { applyReadRls } from '../rls-enforce.ts';
import { fireRowTriggers, fireStatementTriggers, SKIP_ROW } from '../triggers.ts';
import { DeleteStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { MutationDataSourceBase } from './mutation-base.ts';
import { buildCtx } from '../../parser/context.ts';

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
