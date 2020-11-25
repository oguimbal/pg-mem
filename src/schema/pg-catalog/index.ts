
import { _IDb, _ISchema } from '../../interfaces-private';
import { PgAttributeTable } from './pg-attribute-list';
import { PgClassListTable } from './pg-classlist';
import { PgConstraintTable } from './pg-constraints-list';
import { PgIndexTable } from './pg-index-list';
import { PgNamespaceTable } from './pg-namespace-list';
import { PgTypeTable } from './pg-type-list';

export function setupPgCatalog(db: _IDb) {
    const catalog: _ISchema = db.createSchema('pg_catalog');

        catalog._settable('pg_constraint', new PgConstraintTable(catalog))
        catalog._settable('pg_class', new PgClassListTable(catalog))
        catalog._settable('pg_namespace', new PgNamespaceTable(catalog))
        catalog._settable('pg_attribute', new PgAttributeTable(catalog))
        catalog._settable('pg_index', new PgIndexTable(catalog))
        catalog._settable('pg_type', new PgTypeTable(catalog));
}