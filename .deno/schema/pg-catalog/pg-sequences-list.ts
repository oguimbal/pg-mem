import { _ITable, _ISelection, _IIndex, _IDb, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

export class PgSequencesTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_sequences',
        fields: [
            { name: 'schemaname', type: Types.text() }
            , { name: 'sequencename', type: Types.text() }
            , { name: 'sequenceowner', type: Types.integer }
            , { name: 'data_type', type: Types.text() }
            , { name: 'start_value', type: Types.integer }
            , { name: 'min_value', type: Types.integer }
            , { name: 'max_value', type: Types.integer }
            , { name: 'increment_by', type: Types.integer }
            , { name: 'cycle', type: Types.bool }
            , { name: 'cache_size', type: Types.integer }
            , { name: 'last_value', type: Types.integer }
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
