import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

// https://www.postgresql.org/docs/13/catalog-pg-range.html
export class PgRange extends ReadOnlyTable implements _ITable {


    _schema: Schema = {
        name: 'pg_proc',
        fields: [
            // types might be wrong
            { name: 'oid', type: Types.integer }
            , { name: 'proname', type: Types.text() }
            , { name: 'pronamespace', type: Types.text() }
            , { name: 'pronamespace', type: Types.integer }
            , { name: 'proowner', type: Types.integer }
            , { name: 'prolang', type: Types.integer }
            , { name: 'procost', type: Types.integer }
            , { name: 'prorows', type: Types.integer }
            , { name: 'provariadic', type: Types.integer }
            , { name: 'prosupport', type: Types.text() }
            , { name: 'prokind', type: Types.text(1) }
            , { name: 'prosecdef', type: Types.bool }
            , { name: 'proleakproof', type: Types.bool }
            , { name: 'proisstrict', type: Types.bool }
            , { name: 'proretset', type: Types.bool }
            , { name: 'provolatile', type: Types.text(1) }
            , { name: 'pronargs', type: Types.integer }
            , { name: 'pronargdefaults', type: Types.integer }
            , { name: 'prorettype', type: Types.integer }
            , { name: 'proargtypes', type: Types.integer }
            , { name: 'proallargtypes', type: Types.integer.asArray() }
            , { name: 'proargmodes', type: Types.text().asArray() }
            , { name: 'proargnames', type: Types.text().asArray() }
            , { name: 'proargdefaults', type: Types.text() }
            , { name: 'protrftypes', type: Types.text() }
            , { name: 'prosrc', type: Types.text() }
            , { name: 'probin', type: Types.text() }
            , { name: 'prosqlbody', type: Types.text() }
            , { name: 'proconfig', type: Types.text().asArray() }
            , { name: 'proacl', type: Types.text().asArray() }
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
