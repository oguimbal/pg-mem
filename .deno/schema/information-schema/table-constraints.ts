import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';
import { listConstraintRows } from './constraint-rows.ts';

// https://www.postgresql.org/docs/13/catalog-pg-range.html
export class TableConstraints extends ReadOnlyTable implements _ITable {


    _schema: Schema = {
        name: 'table_constraints',
        fields: [
            { name: 'constraint_catalog', type: Types.text() }
            , { name: 'constraint_schema', type: Types.text() }
            , { name: 'constraint_name', type: Types.text() }
            , { name: 'table_catalog', type: Types.text() }
            , { name: 'table_schema', type: Types.text() }
            , { name: 'table_name', type: Types.text() }
            , { name: 'constraint_type', type: Types.text() }
            , { name: 'is_deferrable', type: Types.bool }
            , { name: 'initially_deferred', type: Types.bool }
            , { name: 'enforced', type: Types.bool }
        ]
    };


    entropy(): number {
        return 0;
    }

    *enumerate() {
        for (const c of listConstraintRows(this.db)) {
            const ret = {
                constraint_catalog: 'pgmem',
                constraint_schema: c.schema,
                constraint_name: c.name,
                table_catalog: 'pgmem',
                table_schema: c.schema,
                table_name: c.table,
                constraint_type: c.type,
                is_deferrable: false,
                initially_deferred: false,
                enforced: true,
            };
            yield setId(ret, `/information_schema/table_constraints/${c.schema}/${c.table}/${c.name}`);
        }
    }


    hasItem(value: any): boolean {
        return !!value;
    }

}
