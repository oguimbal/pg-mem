import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types, makeArray } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

export class PgIndexTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_index',
        fields: [
            { name: 'indexrelid', type: Types.int } // oid
            , { name: 'indrelid', type: Types.int } // oid
            , { name: 'indnatts', type: Types.int }
            , { name: 'indnkyatts', type: Types.int }
            , { name: 'indisunique', type: Types.bool }
            , { name: 'indisprimary', type: Types.bool }
            , { name: 'indisxclusion', type: Types.bool }
            , { name: 'indimmediate', type: Types.bool }
            , { name: 'indisclustered', type: Types.bool }
            , { name: 'indisvalid', type: Types.bool }
            , { name: 'indcheckxmin', type: Types.bool }
            , { name: 'indisready', type: Types.bool }
            , { name: 'indisliv', type: Types.bool }
            , { name: 'indisreplident', type: Types.bool }
            , { name: 'indkey', type: makeArray(Types.int) } // int2vector
            , { name: 'indcollation', type: makeArray(Types.int) } // oidvector
            , { name: 'indclass', type: makeArray(Types.int) } // oidvector
            , { name: 'indoption', type: makeArray(Types.int) } // int2vector
            , { name: 'indeexprs', type: Types.jsonb } // pg_node_tree
            , { name: 'indpred', type: Types.jsonb } // pg_node_tree
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
