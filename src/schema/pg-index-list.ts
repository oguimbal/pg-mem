import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _IQuery } from '../interfaces-private';
import { Schema } from '../interfaces';
import { Types, makeArray } from '../datatypes';
import { ReadOnlyTable } from './readonly-table';

export class PgIndexTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_index',
        fields: [
            { id: 'indexrelid', type: Types.int } // oid
            , { id: 'indrelid', type: Types.int } // oid
            , { id: 'indnatts', type: Types.int }
            , { id: 'indnkyatts', type: Types.int }
            , { id: 'indisunique', type: Types.bool }
            , { id: 'indisprimary', type: Types.bool }
            , { id: 'indisxclusion', type: Types.bool }
            , { id: 'indimmediate', type: Types.bool }
            , { id: 'indisclustered', type: Types.bool }
            , { id: 'indisvalid', type: Types.bool }
            , { id: 'indcheckxmin', type: Types.bool }
            , { id: 'indisready', type: Types.bool }
            , { id: 'indisliv', type: Types.bool }
            , { id: 'indisreplident', type: Types.bool }
            , { id: 'indkey', type: makeArray(Types.int) } // int2vector
            , { id: 'indcollation', type: makeArray(Types.int) } // oidvector
            , { id: 'indclass', type: makeArray(Types.int) } // oidvector
            , { id: 'indoption', type: makeArray(Types.int) } // int2vector
            , { id: 'indeexprs', type: Types.jsonb } // pg_node_tree
            , { id: 'indpred', type: Types.jsonb } // pg_node_tree
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
