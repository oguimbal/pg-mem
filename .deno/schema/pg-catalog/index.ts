import { Types } from '../../datatypes.ts';
import { DataType, FunctionDefinition, _IDb, _ISchema } from '../../interfaces-private.ts';
import { PgAttributeTable } from './pg-attribute-list.ts';
import { PgClassListTable } from './pg-classlist.ts';
import { PgConstraintTable } from './pg-constraints-list.ts';
import { PgIndexTable } from './pg-index-list.ts';
import { PgNamespaceTable } from './pg-namespace-list.ts';
import { PgTypeTable } from './pg-type-list.ts';
import { allFunctions } from '../../functions/index.ts';


export function setupPgCatalog(db: _IDb) {
    const catalog: _ISchema = db.createSchema('pg_catalog');

    new PgConstraintTable(catalog).register();
    new PgClassListTable(catalog).register();
    new PgNamespaceTable(catalog).register();
    new PgAttributeTable(catalog).register();
    new PgIndexTable(catalog).register();
    new PgTypeTable(catalog).register();


    // this is an ugly hack...
    const tbl = catalog.declareTable({
        name: 'current_schema',
        fields: [
            { name: 'current_schema', type: Types.text() },
        ]
    }, true);
    tbl.insert(db.data, { current_schema: 'public' });
    tbl.setHidden().setReadonly();

    addFns(catalog, allFunctions);

    catalog.registerFunction({
        name: 'set_config',
        args: [Types.text(), Types.text(), Types.bool],
        returns: Types.text(),
        impure: true,
        implementation: (cfg: string, val: string, is_local: boolean) => {
            // todo - implement this... used to override search_path in dumps.
            //       => have a dynamic search_path.
            //       => not trivial du to the "is_local" arg
            //  https://www.postgresql.org/docs/9.3/functions-admin.html
            return val;
        }
    });

    db.getSchema('pg_catalog').registerFunction({
        name: 'col_description',
        args: [DataType.int, DataType.int],
        returns: DataType.text,
        implementation: x => 'Fake description provided by pg-mem',
    });
    catalog.setReadonly()
}

function addFns(catalog: _ISchema, fns: FunctionDefinition[]) {
    for (const f of fns) {
        catalog.registerFunction(f);
    }
}