import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema } from '../../interfaces-private';
import { Schema } from '../../interfaces';
import { Types } from '../../datatypes';
import { ReadOnlyTable } from '../readonly-table';

// https://www.postgresql.org/docs/13/catalog-pg-range.html
const IS_SCHEMA = Symbol('_is_pg_statio_user_tables');
export class PgStatioUserTables extends ReadOnlyTable implements _ITable {


    _schema: Schema = {
        name: 'pg_statio_user_tables',
        fields: [
            { name: 'relid', type: Types.integer } // oid
            , { name: 'schemaname', type: Types.text() }
            , { name: 'relname', type: Types.text() }
            , { name: 'heap_blks_read', type: Types.integer }
            , { name: 'heap_blks_hit', type: Types.integer }
            , { name: 'idx_blks_read', type: Types.integer }
            , { name: 'idx_blks_hit', type: Types.integer }
            , { name: 'toast_blks_read', type: Types.integer }
            , { name: 'toast_blks_hit', type: Types.integer }
            , { name: 'tidx_blks_read', type: Types.integer }
            , { name: 'tidx_blks_hit', type: Types.integer }

        ]
    };


    entropy(): number {
        return 0;
    }

    *enumerate() {
        for (const t of this.db.public.listTables()) {
            yield {
                relid: t.reg.typeId,
                schemaname: 'public',
                relname: t.name,
                heap_blks_read: 0,
                heap_blks_hit: 0,
                idx_blks_read: 0,
                idx_blks_hit: 0,
                toast_blks_read: 0,
                toast_blks_hit: 0,
                tidx_blks_read: 0,
                tidx_blks_hit: 0,
                [IS_SCHEMA]: true,
            };
        }
    }



    hasItem(value: any): boolean {
        return value[IS_SCHEMA];
    }
}
