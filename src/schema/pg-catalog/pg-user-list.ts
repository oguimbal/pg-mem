import { _ITable, _ISelection, _IIndex, _IDb, _ISchema } from '../../interfaces-private';
import { Schema } from '../../interfaces';
import { Types } from '../../datatypes';
import { ReadOnlyTable } from '../readonly-table';

export class PgUserTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_user',
        fields: [
            { name: 'usename', type: Types.text() }
            , { name: 'usesysid', type: Types.integer }
            , { name: 'usecreatedb', type: Types.bool }
            , { name: 'usesuper', type: Types.bool }
            , { name: 'usecatupd', type: Types.bool }
            , { name: 'userepl', type: Types.bool }
            , { name: 'usebypassrls', type: Types.bool }
            , { name: 'passwd', type: Types.text() }
            , { name: 'valuntil', type: Types.timestamptz() }
            , { name: 'useconfig', type: Types.jsonb }
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
