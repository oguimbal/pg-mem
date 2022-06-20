
import { _IDb, _ISchema } from '../../interfaces-private.ts';
import { ColumnsListSchema } from './columns-list.ts';
import { TablesSchema } from './table-list.ts';
import { TableConstraints } from './table-constraints.ts';
import { KeyColumnUsage } from './key-column-usage.ts';
import { ConstraintColumnUsage } from './constraint-column-usage.ts';

export function setupInformationSchema(db: _IDb) {
    const schema: _ISchema = db.createSchema('information_schema');

    // SELECT * FROM "information_schema"."tables" WHERE ("table_schema" = 'public' AND "table_name" = 'user')
    new TablesSchema(schema).register();
    new ColumnsListSchema(schema).register();
    new TableConstraints(schema).register();
    new KeyColumnUsage(schema).register();
    new ConstraintColumnUsage(schema).register();

    schema.setReadonly();
}