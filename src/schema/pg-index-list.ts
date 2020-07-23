import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId } from '../interfaces-private';
import { Selection } from '../transforms/selection';
import { ReadOnlyError, NotSupported } from '../interfaces';
import { Types, makeArray } from '../datatypes';

export class PgIndexTable implements _ITable {

    hidden = true;

    get name() {
        return 'pg_index';
    }

    selection: _ISelection<any> = new Selection(this, {
        schema: {
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
        }
    });

    constructor(readonly db: _IDb) {
    }

    insert(toInsert: any): void {
        throw new ReadOnlyError('information schema');
    }
    createIndex(): this {
        throw new ReadOnlyError('information schema');
    }

    setReadonly(): this {
        throw new ReadOnlyError('information schema');
    }

    get entropy(): number {
        return 0;
    }

    *enumerate() {
    }



    hasItem(value: any): boolean {
        return false;
    }

    getIndex(forValue: IValue<any>): _IIndex<any> {
        return null;
    }

    on(): void {
        throw new NotSupported('subscribing information schema');
    }

}
