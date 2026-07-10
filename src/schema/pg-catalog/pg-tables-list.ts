import { _ITable, _ISchema, setId } from '../../interfaces-private';
import { Schema } from '../../interfaces';
import { Types } from '../../datatypes';
import { ReadOnlyTable } from '../readonly-table';

// https://www.postgresql.org/docs/current/view-pg-tables.html
export class PgTablesTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_tables',
        fields: [
            { name: 'schemaname', type: Types.text() }
            , { name: 'tablename', type: Types.text() }
            , { name: 'tableowner', type: Types.text() }
            , { name: 'tablespace', type: Types.text() }
            , { name: 'hasindexes', type: Types.bool }
            , { name: 'hasrules', type: Types.bool }
            , { name: 'hastriggers', type: Types.bool }
            , { name: 'rowsecurity', type: Types.bool }
        ]
    };

    entropy(): number {
        return 0;
    }

    *enumerate() {
        for (const schema of this.db.listSchemas()) {
            for (const table of schema.listTables()) {
                const hasindexes = !(table.listIndexes()[Symbol.iterator]().next().done);
                const ret = {
                    schemaname: schema.name,
                    tablename: table.name,
                    tableowner: 'pg_mem',
                    tablespace: null,
                    hasindexes,
                    hasrules: false,
                    hastriggers: table.triggers.triggers.length > 0,
                    rowsecurity: table.rls.enabled,
                };
                yield setId(ret, `/schema/${schema.name}/pg_tables/${table.name}`);
            }
        }
    }

    hasItem(value: any): boolean {
        return !!value;
    }
}
