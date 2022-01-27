import { _ITable, _Transaction, _Explainer, _ISchema, asTable, _ISelection, _IIndex, _IStatement } from '../../interfaces-private';
import { UpdateStatement } from 'pgsql-ast-parser';
import { MutationDataSourceBase, Setter, createSetter } from './mutation-base';

export class Update extends MutationDataSourceBase<any> {

    private setter: Setter;

    constructor(statement: _IStatement, ast: UpdateStatement) {
        const into = asTable(statement.schema.getObject(ast.table));
        const mutatedSel = into
            .selection
            .filter(ast.where);

        super(statement, into, mutatedSel, ast);

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
