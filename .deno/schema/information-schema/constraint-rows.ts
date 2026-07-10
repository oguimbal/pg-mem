import { _IDb } from '../../interfaces-private.ts';

export interface ConstraintRow {
    schema: string;
    table: string;
    name: string;
    type: 'PRIMARY KEY' | 'UNIQUE';
    columns: string[];
}

/** Enumerates the key constraints (primary key + unique) of every table, used to back
 * information_schema.table_constraints and key_column_usage. */
export function* listConstraintRows(db: _IDb): Iterable<ConstraintRow> {
    for (const schema of db.listSchemas()) {
        for (const table of schema.listTables()) {
            const pk = (table as any).primaryIndex as { name: string; expressions: string[] } | null;
            if (pk) {
                yield { schema: schema.name, table: table.name, name: pk.name, type: 'PRIMARY KEY', columns: pk.expressions };
            }
            for (const idx of table.listIndexes()) {
                if (!idx.unique || idx.name === pk?.name) {
                    continue;
                }
                const columns = idx.expressions.map((e: any) => e.id).filter((c: any) => !!c);
                yield { schema: schema.name, table: table.name, name: idx.name, type: 'UNIQUE', columns };
            }
        }
    }
}
