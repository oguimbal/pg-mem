import { _ITable, _ISchema, setId } from '../../interfaces-private';
import { Schema } from '../../interfaces';
import { Types } from '../../datatypes';
import { ReadOnlyTable } from '../readonly-table';

// https://www.postgresql.org/docs/current/view-pg-indexes.html
export class PgIndexesTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_indexes',
        fields: [
            { name: 'schemaname', type: Types.text() }
            , { name: 'tablename', type: Types.text() }
            , { name: 'indexname', type: Types.text() }
            , { name: 'tablespace', type: Types.text() }
            , { name: 'indexdef', type: Types.text() }
        ]
    };

    entropy(): number {
        return 0;
    }

    *enumerate() {
        for (const schema of this.db.listSchemas()) {
            for (const table of schema.listTables()) {
                for (const index of table.listIndexes()) {
                    // index expressions are IValues at runtime; `.id` is the column name
                    const cols = index.expressions.map(e => (e as any).id ?? '?').join(', ');
                    const def = `CREATE ${index.unique ? 'UNIQUE ' : ''}INDEX ${index.name} `
                        + `ON ${schema.name}.${table.name} USING btree (${cols})`;
                    const ret = {
                        schemaname: schema.name,
                        tablename: table.name,
                        indexname: index.name,
                        tablespace: null,
                        indexdef: def,
                    };
                    yield setId(ret, `/schema/${schema.name}/pg_indexes/${table.name}/${index.name}`);
                }
            }
        }
    }

    hasItem(value: any): boolean {
        return !!value;
    }
}
