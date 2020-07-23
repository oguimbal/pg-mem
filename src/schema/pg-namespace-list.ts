import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _IQuery } from '../interfaces-private';
import { Selection } from '../transforms/selection';
import { ReadOnlyError, NotSupported } from '../interfaces';
import { Types } from '../datatypes';

export class PgNamespaceTable implements _ITable {

    hidden = true;

    get name() {
        return 'pg_namespace';
    }

    selection: _ISelection<any> = new Selection(this, {
        schema: {
            name: 'pg_namespace',
            fields: [
                { id: 'oid', type: Types.int } // hidden oid column
                , { id: 'nspname', type: Types.text() }
                , { id: 'nspowner', type: Types.int } // oid
                , { id: 'nspacl', type: Types.jsonb } // aclitem[]
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

        // yield {
        //     oid: MAIN_NAMESPACE,
        //     nspname: 'public',
        //     nspowner: null,
        //     nspacl: null,
        // };
        // yield {
        //     oid: MAIN_NAMESPACE,
        //     nspname: 'public',
        //     nspowner: null,
        //     nspacl: null,
        // };
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
