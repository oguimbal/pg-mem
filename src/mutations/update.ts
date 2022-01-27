import { _ITable, _Transaction, _Explainer, _ISchema, asTable, _ISelection, _IIndex } from '../interfaces-private';
import { UpdateStatement } from 'pgsql-ast-parser';
import { MutationDataSourceBase, Setter, createSetter } from './mutation-base';

export class Update extends MutationDataSourceBase<any> {

    private setter: Setter;

    constructor(schema: _ISchema, statement: UpdateStatement) {
        const into = asTable(schema.getObject(statement.table));
        const mutatedSel = into
            .selection
            .filter(statement.where);

        super(into, mutatedSel, statement);

        this.setter = createSetter(this.table, this.mutatedSel, statement.sets);

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
