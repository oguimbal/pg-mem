import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

export class PgTypeTable extends ReadOnlyTable implements _ITable {


    _schema: Schema = {
        name: 'pg_type',
        fields: [
            { name: 'oid', type: Types.integer } // hiddn oid column
            , { name: 'typname', type: Types.text() }
            , { name: 'typnamespace', type: Types.integer } // oid
            , { name: 'typowner', type: Types.integer } // oid
            , { name: 'typlen', type: Types.integer }
            , { name: 'typbyval', type: Types.bool }
            , { name: 'typtype', type: Types.text(1) } // char(1)
            , { name: 'typispreferred', type: Types.bool }
            , { name: 'typisdefined', type: Types.bool }
            , { name: 'typdlim', type: Types.text(1) } // char(1)
            , { name: 'typrelid', type: Types.integer } // oid
            , { name: 'typelem', type: Types.integer } // oid
            , { name: 'typarray', type: Types.integer } // oid
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
            , { name: 'typbasetype', type: Types.integer } //oid
            , { name: 'typtypmod', type: Types.integer }
            , { name: 'typndims', type: Types.integer }
            , { name: 'typcollation', type: Types.integer } // oid
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
