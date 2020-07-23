import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _IQuery } from '../interfaces-private';
import { Selection } from '../transforms/selection';
import { ReadOnlyError, NotSupported } from '../interfaces';
import { Types, makeArray } from '../datatypes';

export class PgTypeTable implements _ITable {

    hidden = true;

    get name() {
        return 'pg_type';
    }

    selection: _ISelection<any> = new Selection(this, {
        schema: {
            name: 'pg_type',
            fields: [
                { id: 'oid', type: Types.int } // hiddn oid column
                , { id: 'typname', type: Types.text() }
                , { id: 'typnamespace', type: Types.int } // oid
                , { id: 'typowner', type: Types.int } // oid
                , { id: 'typlen', type: Types.int }
                , { id: 'typbyval', type: Types.bool }
                , { id: 'typtype', type: Types.text(1) } // char(1)
                , { id: 'typispreferred', type: Types.bool }
                , { id: 'typisdefined', type: Types.bool }
                , { id: 'typdlim', type: Types.text(1) } // char(1)
                , { id: 'typrelid', type: Types.int } // oid
                , { id: 'typelem', type: Types.int } // oid
                , { id: 'typarray', type: Types.int } // oid
                , { id: 'typinput', type: Types.text() } // regproc
                , { id: 'typoutput', type: Types.text() } // regproc
                , { id: 'typreceive', type: Types.text() } // regproc
                , { id: 'typsend', type: Types.text() } // regproc
                , { id: 'typmodin', type: Types.text() } // regproc
                , { id: 'typmodout', type: Types.text() } // regproc
                , { id: 'typanalyze', type: Types.text() } // regproc
                , { id: 'typalign', type: Types.text(1) } // char(1)
                , { id: 'typstorage', type: Types.text(1) } // char(1)
                , { id: 'typnotnull', type: Types.bool }
                , { id: 'typbasetype', type: Types.int } //oid
                , { id: 'typtypmod', type: Types.int }
                , { id: 'typndims', type: Types.int }
                , { id: 'typcollation', type: Types.int } // oid
                , { id: 'typdfaultbin', type: Types.text() } // pg_nod_tree
                , { id: 'typdefault', type: Types.text() }
                , { id: 'typacl', type: Types.jsonb }
            ]
        }
    });

    constructor(readonly schema: _IQuery) {
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

    entropy(): number {
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
