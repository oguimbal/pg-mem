import { Types } from '../../datatypes.ts';
import { _IDb, _ISchema } from '../../interfaces-private.ts';
import { PgAttributeTable } from './pg-attribute-list.ts';
import { PgClassListTable } from './pg-classlist.ts';
import { PgConstraintTable } from './pg-constraints-list.ts';
import { PgIndexTable } from './pg-index-list.ts';
import { PgNamespaceTable } from './pg-namespace-list.ts';
import { PgTypeTable } from './pg-type-list.ts';
import { stringFunctions } from '../../functions/string.ts';
import { dateFunctions } from '../../functions/date.ts';
import { systemFunctions } from '../../functions/system.ts';
import { FunctionDefinition } from '../../interfaces.ts';

export function setupPgCatalog(db: _IDb) {
    const catalog: _ISchema = db.createSchema('pg_catalog');

        catalog._settable('pg_constraint', new PgConstraintTable(catalog))
        catalog._settable('pg_class', new PgClassListTable(catalog))
        catalog._settable('pg_namespace', new PgNamespaceTable(catalog))
        catalog._settable('pg_attribute', new PgAttributeTable(catalog))
        catalog._settable('pg_index', new PgIndexTable(catalog))
        catalog._settable('pg_type', new PgTypeTable(catalog));


        // this is an ugly hack...
        const tbl = catalog.declareTable({
            name: 'current_schema',
            fields: [
                { name: 'current_schema', type: Types.text() },
            ]
        }, true);
        tbl.insert(db.data, { current_schema: 'public' });
        tbl.setHidden().setReadonly();

        addFns(catalog, stringFunctions);
        addFns(catalog, dateFunctions);
        addFns(catalog, systemFunctions);
    }

    function addFns(catalog: _ISchema, fns: FunctionDefinition[]) {
        for (const f of fns) {
            catalog.registerFunction(f);
        }
    }