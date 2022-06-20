import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

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
    }


    hasItem(value: any): boolean {
        return false;
    }

}
