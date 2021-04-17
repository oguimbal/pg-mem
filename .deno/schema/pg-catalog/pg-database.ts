import { _ITable, _ISelection, _IIndex, _IDb, _ISchema, _Transaction, setId } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

// https://www.postgresql.org/docs/12/catalog-pg-class.html

const IS_SCHEMA = Symbol('_is_pg_database');
export class PgDatabaseTable extends ReadOnlyTable implements _ITable {

    get ownSymbol() {
        return IS_SCHEMA;
    }


    _schema: Schema = {
        name: 'pg_database',
        fields: [
            { name: 'oid', type: Types.integer } // hidden oid column
            , { name: 'datname', type: Types.text() }
            , { name: 'datdba', type: Types.integer }
            , { name: 'encoding', type: Types.integer }
            , { name: 'datcollate', type: Types.text() }
            , { name: 'datctype', type: Types.text() }
            , { name: 'datistemplate', type: Types.bool }
            , { name: 'datlowconn', type: Types.bool }
            , { name: 'datconlimit', type: Types.integer }
            , { name: 'datlastsysoid', type: Types.integer }
            , { name: 'datfrozenxid', type: Types.integer }
            , { name: 'datminmxid', type: Types.integer }
            , { name: 'dattablespace', type: Types.integer }
            , { name: 'datacl', type: Types.jsonb }
        ]
    };

    entropy(t: _Transaction): number {
        return this.db.listSchemas().length;
    }

    *enumerate() {
        // this is 💩, whaterver...
        let i = 48593;
        for (const t of this.db.listSchemas()) {
            const ret = {
                oid: ++i,
                datname: t.name,
                [IS_SCHEMA]: true,
            };
            yield setId(ret, '/schema/pg_database/' + t.name);
        }
    }


    hasItem(value: any): boolean {
        return !!value?.[IS_SCHEMA];
    }
}
