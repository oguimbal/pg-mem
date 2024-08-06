import { DataType, newDb } from './src';

// attempt to make tableplus or dbeaver working on pg-mem
// ...still things failing

const db = newDb();

db.public.query(`
            create table users (id serial primary key, name text, is_ok boolean, data jsonb);
            insert into users (name, is_ok, data) values
                ('Alice', true, '{"gender":"female"}'),
                ('Bob', false, null),
                ('Anon', null, null);
                `);


db.public.registerFunction({
    name: 'version',
    returns: DataType.text,
    implementation: () => `PostgreSQL 16.3 (Debian 16.3-1.pgdg120+1) on aarch64-unknown-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit`
})


db.getSchema('pg_catalog').registerFunction({
    name: 'pg_get_userbyid',
    args: [DataType.integer],
    returns: DataType.text,
    implementation: () => 'pgmem'
});
db.getSchema('pg_catalog').registerFunction({
    name: 'obj_description',
    args: [DataType.integer, DataType.text],
    returns: DataType.text,
    implementation: () => null,
});
db.getSchema('pg_catalog').registerFunction({
    name: 'pg_total_relation_size',
    args: [DataType.integer],
    returns: DataType.integer,
    implementation: () => 0,
});
db.getSchema('pg_catalog').registerFunction({
    name: 'pg_table_size',
    args: [DataType.integer],
    returns: DataType.integer,
    implementation: () => 0,
});
db.getSchema('pg_catalog').registerFunction({
    name: 'pg_indexes_size',
    args: [DataType.integer],
    returns: DataType.integer,
    implementation: () => 0,
});
db.getSchema('pg_catalog').registerFunction({
    name: 'pg_get_function_identity_arguments',
    args: [DataType.integer],
    returns: DataType.text,
    implementation: () => 'pgmem'
});

db.public.interceptQueries(sql => {

    // see https://github.com/pgjdbc/pgjdbc/blob/fc60537c8e2c40b7da6a952ca2ba4a12f2d5ae86/pgjdbc/src/main/java/org/postgresql/jdbc/PgConnection.java#L820
    // and https://github.com/pgjdbc/pgjdbc/blob/fc60537c8e2c40b7da6a952ca2ba4a12f2d5ae86/pgjdbc/src/main/java/org/postgresql/jdbc/TypeInfoCache.java#L252
    if (sql.includes('current_schemas(')) {
        return [{ oid: 603, typname: 'box' }];
    }
    return undefined;
})

db.adapters.bindServer({ port: 52932 })
    .then(v => console.log('Server at', v))
    .catch(e => console.error('Error', e));
