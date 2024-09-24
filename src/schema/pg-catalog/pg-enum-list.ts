import { _ITable, _ISelection, _IIndex, _IDb, _ISchema } from '../../interfaces-private';
import { Schema } from '../../interfaces';
import { Types } from '../../datatypes';
import { ReadOnlyTable } from '../readonly-table';

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
