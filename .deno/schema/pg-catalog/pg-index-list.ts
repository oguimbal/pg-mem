import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

export class PgIndexTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_index',
        fields: [
            { name: 'indexrelid', type: Types.integer } // oid
            , { name: 'indrelid', type: Types.integer } // oid
            , { name: 'indnatts', type: Types.integer }
            , { name: 'indnkyatts', type: Types.integer }
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
            , { name: 'indkey', type: Types.integer.asArray() } // int2vector
            , { name: 'indcollation', type: Types.integer.asArray() } // oidvector
            , { name: 'indclass', type: Types.integer.asArray() } // oidvector
            , { name: 'indoption', type: Types.integer.asArray() } // int2vector
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
