import { _ITable, _Transaction, IValue, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private';
import { DeleteStatement } from 'pgsql-ast-parser';
import { MutationDataSourceBase } from './mutation-base';

export class Deletion extends MutationDataSourceBase<any> {


    constructor(statement: _IStatement, ast: DeleteStatement) {
        const table = asTable(statement.schema.getObject(ast.from));
        const mutatedSel = table
            .selection
            .filter(ast.where);

        super(statement, table, mutatedSel, ast);
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
