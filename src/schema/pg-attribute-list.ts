import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId } from '../interfaces-private';
import { Selection } from '../transforms/selection';
import { ReadOnlyError, NotSupported } from '../interfaces';
import { Types, makeArray } from '../datatypes';

export class PgAttributeTable implements _ITable {

    hidden = true;

    get name() {
        return 'pg_attribute';
    }

    selection: _ISelection<any> = new Selection(this, {
        schema: {
            name: 'pg_attribute',
            fields: [
                { id: 'attrelid', type: Types.int } // oid
                , { id: 'attname', type: Types.text() }
                , { id: 'atttypid', type: Types.int } // oid
                , { id: 'attstattarget', type: Types.int }
                , { id: 'attlen', type: Types.int }
                , { id: 'attnum', type: Types.int }
                , { id: 'attndims', type: Types.int }
                , { id: 'attcacheoff', type: Types.int }
                , { id: 'atttypmod', type: Types.int }
                , { id: 'attbyval', type: Types.bool }
                , { id: 'attstorage', type: Types.text(1) } // char(1)
                , { id: 'attalign', type: Types.text(1) } // char(1)
                , { id: 'attnotnull', type: Types.bool }
                , { id: 'atthasdef', type: Types.bool }
                , { id: 'atthasmissing', type: Types.bool }
                , { id: 'attidntity', type: Types.text(1) } // char(1)
                , { id: 'attisdroppd', type: Types.bool }
                , { id: 'attislocal', type: Types.bool }
                , { id: 'attinhcount', type: Types.int }
                , { id: 'attcollation', type: Types.int } // oid
                , { id: 'attacl', type: Types.jsonb } // aclitem[]
                , { id: 'attoptions', type: makeArray(Types.text()) }
                , { id: 'attfdwoptions', type: makeArray(Types.text()) }
                , { id: 'attmissingval', type: Types.jsonb }// anyarray
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
