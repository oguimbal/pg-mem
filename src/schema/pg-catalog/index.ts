import { Types } from '../../datatypes';
import { DataType, FunctionDefinition, _IDb, _ISchema } from '../../interfaces-private';
import { PgAttributeTable } from './pg-attribute-list';
import { PgClassListTable } from './pg-class';
import { PgConstraintTable } from './pg-constraints-list';
import { PgIndexTable } from './pg-index-list';
import { PgNamespaceTable } from './pg-namespace-list';
import { PgTypeTable } from './pg-type-list';
import { allFunctions } from '../../functions';
import { PgRange } from './pg-range';
import { sqlSubstring } from '../../parser/expression-builder';
import { PgDatabaseTable } from './pg-database';
import { registerCommonOperators } from './binary-operators';
import { registerSqlFunctionLanguage } from './sql-function-language';


export function setupPgCatalog(db: _IDb) {
    const catalog: _ISchema = db.createSchema('pg_catalog');

    catalog._registerType(Types.bool)
        ._registerType(Types.citext)
        ._registerTypeSizeable(DataType.timestamp, Types.timestamp)
        ._registerType(Types.uuid)
        ._registerType(Types.date)
        ._registerType(Types.time)
        ._registerType(Types.jsonb)
        ._registerType(Types.regtype)
        ._registerType(Types.regclass)
        ._registerType(Types.json)
        ._registerType(Types.null)
        ._registerType(Types.float)
        ._registerType(Types.integer)
        ._registerType(Types.bigint)
        ._registerType(Types.bytea)
        ._registerType(Types.point)
        ._registerType(Types.line)
        ._registerType(Types.lseg)
        ._registerType(Types.box)
        ._registerType(Types.path)
        ._registerType(Types.polygon)
        ._registerType(Types.circle)
        ._registerType(Types.interval)
        ._registerType(Types.inet)
        ._registerTypeSizeable(DataType.text, Types.text)

    new PgConstraintTable(catalog).register();
    new PgClassListTable(catalog).register();
    new PgNamespaceTable(catalog).register();
    new PgAttributeTable(catalog).register();
    new PgIndexTable(catalog).register();
    new PgTypeTable(catalog).register();
    new PgRange(catalog).register();
    new PgDatabaseTable(catalog).register();


    // this is an ugly hack...
    const tbl = catalog.declareTable({
        name: 'current_schema',
        fields: [
            { name: 'current_schema', type: Types.text() },
        ]
    }, true);
    tbl.insert({ current_schema: 'public' });
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

    catalog.registerFunction({
        name: 'substring',
        args: [Types.text(), Types.integer],
        returns: Types.text(),
        implementation: sqlSubstring,
    })

    catalog.registerFunction({
        name: 'substring',
        args: [Types.text(), Types.integer, Types.integer],
        returns: Types.text(),
        implementation: sqlSubstring,
    })


    db.getSchema('pg_catalog').registerFunction({
        name: 'col_description',
        args: [DataType.integer, DataType.integer],
        returns: DataType.text,
        implementation: x => 'Fake description provided by pg-mem',
    });

    registerCommonOperators(catalog);


    registerSqlFunctionLanguage(db);

    catalog.setReadonly()
}

function addFns(catalog: _ISchema, fns: FunctionDefinition[]) {
    for (const f of fns) {
        catalog.registerFunction(f);
    }
}