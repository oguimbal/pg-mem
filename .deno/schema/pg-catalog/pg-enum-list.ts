import { _ITable, _ISelection, _IIndex, _IDb, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

export class PgEnumTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_enum',
        fields: [
            { name: 'oid', type: Types.integer }
            , { name: 'enumtypid', type: Types.integer }
            , { name: 'enumsortorder', type: Types.integer }
            , { name: 'enumlabel', type: Types.text() }
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
