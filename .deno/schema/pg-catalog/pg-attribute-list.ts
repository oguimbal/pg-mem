import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import {  nil, Schema } from '../../interfaces.ts';
import { Types, makeArray } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

export class PgAttributeTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_attribute',
        fields: [
            { name: 'attrelid', type: Types.int } // oid
            , { name: 'attname', type: Types.text() }
            , { name: 'atttypid', type: Types.int } // oid
            , { name: 'attstattarget', type: Types.int }
            , { name: 'attlen', type: Types.int }
            , { name: 'attnum', type: Types.int }
            , { name: 'attndims', type: Types.int }
            , { name: 'attcacheoff', type: Types.int }
            , { name: 'atttypmod', type: Types.int }
            , { name: 'attbyval', type: Types.bool }
            , { name: 'attstorage', type: Types.text(1) } // char(1)
            , { name: 'attalign', type: Types.text(1) } // char(1)
            , { name: 'attnotnull', type: Types.bool }
            , { name: 'atthasdef', type: Types.bool }
            , { name: 'atthasmissing', type: Types.bool }
            , { name: 'attidntity', type: Types.text(1) } // char(1)
            , { name: 'attisdroppd', type: Types.bool }
            , { name: 'attislocal', type: Types.bool }
            , { name: 'attinhcount', type: Types.int }
            , { name: 'attcollation', type: Types.int } // oid
            , { name: 'attacl', type: Types.jsonb } // aclitem[]
            , { name: 'attoptions', type: makeArray(Types.text()) }
            , { name: 'attfdwoptions', type: makeArray(Types.text()) }
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
