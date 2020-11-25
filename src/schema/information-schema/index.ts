
import { _IDb, _ISchema } from '../../interfaces-private';
import { ColumnsListSchema } from './columns-list';
import { TablesSchema } from './table-list';

export function setupInformationSchema(db: _IDb) {
    const schema: _ISchema = db.createSchema('information_schema');

    // SELECT * FROM "information_schema"."tables" WHERE ("table_schema" = 'public' AND "table_name" = 'user')
    schema._settable('tables', new TablesSchema(schema));
    schema._settable('columns', new ColumnsListSchema(schema));
}