import { _ITable, _Transaction, IValue, _Explainer, _ISchema, asTable, _ISelection, _IIndex } from '../interfaces-private';
import { DeleteStatement } from 'pgsql-ast-parser';
import { MutationDataSourceBase } from './mutation-base';

export class Deletion extends MutationDataSourceBase<any> {

    constructor(schema: _ISchema, statement: DeleteStatement) {
        const table = asTable(schema.getObject(statement.from));
        const mutatedSel = table
            .selection
            .filter(statement.where);

        super(table, mutatedSel, statement);
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
