import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { nil, Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

export class PgAttributeTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_attribute',
        fields: [
            { name: 'attrelid', type: Types.integer } // oid
            , { name: 'attname', type: Types.text() }
            , { name: 'atttypid', type: Types.integer } // oid
            , { name: 'attstattarget', type: Types.integer }
            , { name: 'attlen', type: Types.integer }
            , { name: 'attnum', type: Types.integer }
            , { name: 'attndims', type: Types.integer }
            , { name: 'attcacheoff', type: Types.integer }
            , { name: 'atttypmod', type: Types.integer }
            , { name: 'attbyval', type: Types.bool }
            , { name: 'attstorage', type: Types.text(1) } // char(1)
            , { name: 'attalign', type: Types.text(1) } // char(1)
            , { name: 'attnotnull', type: Types.bool }
            , { name: 'atthasdef', type: Types.bool }
            , { name: 'atthasmissing', type: Types.bool }
            , { name: 'attidntity', type: Types.text(1) } // char(1)
            , { name: 'attisdropped', type: Types.bool }
            , { name: 'attislocal', type: Types.bool }
            , { name: 'attinhcount', type: Types.integer }
            , { name: 'attcollation', type: Types.integer } // oid
            , { name: 'attacl', type: Types.jsonb } // aclitem[]
            , { name: 'attoptions', type: Types.text().asArray() }
            , { name: 'attfdwoptions', type: Types.text().asArray() }
            , { name: 'attmissingval', type: Types.jsonb }// anyarray
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

    getIndex(forValue: IValue<any>): _IIndex<any> | nil {
        return null;
    }

}
