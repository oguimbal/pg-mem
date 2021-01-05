import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

// https://www.postgresql.org/docs/13/catalog-pg-range.html
export class PgRange extends ReadOnlyTable implements _ITable {


    _schema: Schema = {
        name: 'pg_range',
        fields: [
            { name: 'rngtypid', type: Types.integer } // oid
            , { name: 'rngsubtype', type: Types.integer } // oid
            , { name: 'rngcollation', type: Types.integer } // oid
            , { name: 'rngsubopc', type: Types.integer } // oid
            , { name: 'rngcanonical', type: Types.integer } // oid
            , { name: 'rngsubdiff', type: Types.integer } // oid
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
