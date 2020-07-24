import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _IQuery } from '../interfaces-private';
import { ReadOnlyError, NotSupported, Schema } from '../interfaces';
import { Types } from '../datatypes';
import { ReadOnlyTable } from './readonly-table';

export class PgNamespaceTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_namespace',
        fields: [
            { id: 'oid', type: Types.int } // hidden oid column
            , { id: 'nspname', type: Types.text() }
            , { id: 'nspowner', type: Types.int } // oid
            , { id: 'nspacl', type: Types.jsonb } // aclitem[]
        ]
    };


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
}
