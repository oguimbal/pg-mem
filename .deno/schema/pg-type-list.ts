import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../interfaces-private.ts';
import { ReadOnlyError, NotSupported, Schema } from '../interfaces.ts';
import { Types, makeArray } from '../datatypes.ts';
import { ReadOnlyTable } from './readonly-table.ts';

export class PgTypeTable extends ReadOnlyTable implements _ITable {


    _schema: Schema = {
        name: 'pg_type',
        fields: [
            { name: 'oid', type: Types.int } // hiddn oid column
            , { name: 'typname', type: Types.text() }
            , { name: 'typnamespace', type: Types.int } // oid
            , { name: 'typowner', type: Types.int } // oid
            , { name: 'typlen', type: Types.int }
            , { name: 'typbyval', type: Types.bool }
            , { name: 'typtype', type: Types.text(1) } // char(1)
            , { name: 'typispreferred', type: Types.bool }
            , { name: 'typisdefined', type: Types.bool }
            , { name: 'typdlim', type: Types.text(1) } // char(1)
            , { name: 'typrelid', type: Types.int } // oid
            , { name: 'typelem', type: Types.int } // oid
            , { name: 'typarray', type: Types.int } // oid
            , { name: 'typinput', type: Types.text() } // regproc
            , { name: 'typoutput', type: Types.text() } // regproc
            , { name: 'typreceive', type: Types.text() } // regproc
            , { name: 'typsend', type: Types.text() } // regproc
            , { name: 'typmodin', type: Types.text() } // regproc
            , { name: 'typmodout', type: Types.text() } // regproc
            , { name: 'typanalyze', type: Types.text() } // regproc
            , { name: 'typalign', type: Types.text(1) } // char(1)
            , { name: 'typstorage', type: Types.text(1) } // char(1)
            , { name: 'typnotnull', type: Types.bool }
            , { name: 'typbasetype', type: Types.int } //oid
            , { name: 'typtypmod', type: Types.int }
            , { name: 'typndims', type: Types.int }
            , { name: 'typcollation', type: Types.int } // oid
            , { name: 'typdfaultbin', type: Types.text() } // pg_nod_tree
            , { name: 'typdefault', type: Types.text() }
            , { name: 'typacl', type: Types.jsonb }
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
