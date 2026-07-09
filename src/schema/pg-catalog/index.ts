import { Types } from '../../datatypes';
import { DataType, FunctionDefinition, _IDb, _ISchema, GLOBAL_VARS, QueryError } from '../../interfaces-private';
import { executionCtx } from '../../utils';
import { PgAttributeTable } from './pg-attribute-list';
import { PgClassListTable } from './pg-class';
import { PgConstraintTable } from './pg-constraints-list';
import { PgEnumTable } from './pg-enum-list';
import { PgIndexTable } from './pg-index-list';
import { PgNamespaceTable } from './pg-namespace-list';
import { PgSequencesTable } from './pg-sequences-list';
import { PgTypeTable } from './pg-type-list';
import { PgUserTable } from './pg-user-list';
import { PgPoliciesTable } from './pg-policies-list';
import { PgIndexesTable } from './pg-indexes-list';
import { PgTablesTable } from './pg-tables-list';
import { allFunctions } from '../../functions';
import { PgRange } from './pg-range';
import { sqlSubstring } from '../../parser/expression-builder';
import { PgDatabaseTable } from './pg-database';
import { registerCommonOperators } from './binary-operators';
import { registerSqlFunctionLanguage } from './sql-function-language';
import { registerPlpgsqlLanguage } from '../../execution/plpgsql';
import { PgProc } from './pg-proc';
import { PgStatioUserTables } from './pg_statio_user_tables';


export function setupPgCatalog(db: _IDb) {
    const catalog: _ISchema = db.createSchema('pg_catalog');

    catalog._registerType(Types.bool)
        ._registerType(Types.citext)
        ._registerTypeSizeable(DataType.timestamp, Types.timestamp)
        ._registerTypeSizeable(DataType.timestamptz, Types.timestamptz)
        ._registerType(Types.uuid)
        ._registerType(Types.date)
        ._registerType(Types.time)
        ._registerType(Types.timetz)
        ._registerType(Types.jsonb)
        ._registerType(Types.regtype)
        ._registerType(Types.regclass)
        ._registerType(Types.json)
        ._registerType(Types.null)
        ._registerType(Types.float)
        ._registerType(Types.integer)
        ._registerType(Types.bigint)
        ._registerTypeSizeable(DataType.decimal, Types.decimal)
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
        ._registerType(Types.record([])) // hack to support functions with record input (see row_to_json UT)
        ._registerTypeSizeable(DataType.text, Types.text)

    new PgConstraintTable(catalog).register();
    new PgClassListTable(catalog).register();
    new PgNamespaceTable(catalog).register();
    new PgAttributeTable(catalog).register();
    new PgIndexTable(catalog).register();
    new PgTypeTable(catalog).register();
    new PgRange(catalog).register();
    new PgProc(catalog).register();
    new PgDatabaseTable(catalog).register();
    new PgStatioUserTables(catalog).register();
    new PgEnumTable(catalog).register();
    new PgSequencesTable(catalog).register();
    new PgUserTable(catalog).register();
    new PgPoliciesTable(catalog).register();
    new PgIndexesTable(catalog).register();
    new PgTablesTable(catalog).register();


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

    // set_config / current_setting share the per-transaction GLOBAL_VARS store (the same
    // one SET writes to). This is what Supabase RLS relies on: set_config('request.jwt.
    // claims', ...) then current_setting('request.jwt.claims', true) inside a policy.
    // nb: the is_local distinction is not modelled (settings persist for the session).
    catalog.registerFunction({
        name: 'set_config',
        args: [Types.text(), Types.text(), Types.bool],
        returns: Types.text(),
        impure: true,
        allowNullArguments: true,
        implementation: (cfg: string, val: string, is_local: boolean) => {
            const t = executionCtx().transaction;
            t.set(GLOBAL_VARS, t.getMap(GLOBAL_VARS).set(cfg, val));
            return val;
        }
    });

    const readSetting = (name: string, missingOk: boolean) => {
        const v = executionCtx().transaction.getMap(GLOBAL_VARS).get(name);
        if (v === undefined || v === null) {
            if (missingOk) { return null; }
            throw new QueryError(`unrecognized configuration parameter "${name}"`, '42704');
        }
        return v;
    };
    catalog.registerFunction({
        name: 'current_setting',
        args: [Types.text()],
        returns: Types.text(),
        impure: true,
        implementation: (name: string) => readSetting(name, false),
    });
    catalog.registerFunction({
        name: 'current_setting',
        args: [Types.text(), Types.bool],
        returns: Types.text(),
        impure: true,
        implementation: (name: string, missingOk: boolean) => readSetting(name, !!missingOk),
    });

    // UUID generation (pgcrypto / uuid-ossp; core in modern postgres). Used by Supabase's
    // default `id uuid primary key default gen_random_uuid()`.
    const uuidv4 = () => 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
        const r = Math.random() * 16 | 0;
        return (c === 'x' ? r : (r & 0x3 | 0x8)).toString(16);
    });
    for (const name of ['gen_random_uuid', 'uuid_generate_v4']) {
        catalog.registerFunction({
            name,
            args: [],
            returns: Types.uuid,
            impure: true,
            implementation: uuidv4,
        });
    }

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
    });

    catalog.registerFunction({
        // required for Sequelize introspection
        name: 'pg_get_indexdef',
        args: [Types.integer],
        returns: Types.text(),
        implementation: (indexId: number) => {
            throw new Error('This stub implementation of "pg_get_indexdef" should not be called');
        },
    })


    db.getSchema('pg_catalog').registerFunction({
        name: 'col_description',
        args: [DataType.integer, DataType.integer],
        returns: DataType.text,
        implementation: x => 'Fake description provided by pg-mem',
    });

    registerCommonOperators(catalog);


    registerSqlFunctionLanguage(db);
    registerPlpgsqlLanguage(db);

    // Extensions can't ship native code here, but their DDL should not fail (Supabase and
    // many migrations `create extension if not exists ...`). Register the common ones as
    // no-ops; the functions they'd provide that pg-mem needs (gen_random_uuid, ...) are
    // registered as builtins above.
    for (const ext of [
        'pgcrypto', 'uuid-ossp', 'pgjwt', 'pgsodium', 'pg_graphql', 'pg_stat_statements',
        'pg_net', 'supabase_vault', 'citext', 'hstore', 'pg_trgm', 'unaccent',
        'btree_gin', 'btree_gist', 'moddatetime', 'postgis',
    ]) {
        db.registerExtension(ext, () => { /* no-op: no native code in-memory */ });
    }

    catalog.setReadonly()
}

function addFns(catalog: _ISchema, fns: FunctionDefinition[]) {
    for (const f of fns) {
        catalog.registerFunction(f);
    }
}