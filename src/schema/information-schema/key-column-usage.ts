import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private';
import { Schema } from '../../interfaces';
import { Types } from '../../datatypes';
import { ReadOnlyTable } from '../readonly-table';
import { listConstraintRows } from './constraint-rows';


export class KeyColumnUsage extends ReadOnlyTable implements _ITable {


    _schema: Schema = {
        name: 'key_column_usage',
        fields: [
            { name: 'constraint_catalog', type: Types.text() }
            , { name: 'constraint_schema', type: Types.text() }
            , { name: 'constraint_name', type: Types.text() }
            , { name: 'table_catalog', type: Types.text() }
            , { name: 'table_schema', type: Types.text() }
            , { name: 'table_name', type: Types.text() }
            , { name: 'column_name', type: Types.text() }
            , { name: 'ordinal_position', type: Types.integer }
            , { name: 'position_in_unique_constraint', type: Types.integer }
        ]
    };


    entropy(): number {
        return 0;
    }

    *enumerate() {
        for (const c of listConstraintRows(this.db)) {
            for (let i = 0; i < c.columns.length; i++) {
                const ret = {
                    constraint_catalog: 'pgmem',
                    constraint_schema: c.schema,
                    constraint_name: c.name,
                    table_catalog: 'pgmem',
                    table_schema: c.schema,
                    table_name: c.table,
                    column_name: c.columns[i],
                    ordinal_position: i + 1,
                    position_in_unique_constraint: null,
                };
                yield setId(ret, `/information_schema/key_column_usage/${c.schema}/${c.table}/${c.name}/${c.columns[i]}`);
            }
        }
    }


    hasItem(value: any): boolean {
        return !!value;
    }

}
