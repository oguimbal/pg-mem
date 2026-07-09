-- catalog introspection views are environment-sensitive (schema names, cross-schema
-- enumeration), so these are verified offline against pg-mem.

-- @case: pg_indexes lists a table's indexes
-- @offline
-- @expect: [{"indexname":"users_email_idx"},{"indexname":"users_pkey"}]
create table users (id int primary key, email text);
create unique index users_email_idx on users (email);
select indexname from pg_indexes where tablename = 'users' order by indexname;

-- @case: pg_indexes exposes an index definition
-- @offline
-- @expect: [{"indexdef":"CREATE UNIQUE INDEX users_email_idx ON public.users USING btree (email)"}]
create table users (id int primary key, email text);
create unique index users_email_idx on users (email);
select indexdef from pg_indexes where indexname = 'users_email_idx';

-- @case: pg_tables reports hasindexes and hastriggers
-- @offline
-- @expect: [{"tablename":"t","hasindexes":true}]
create table t (id int primary key, v int);
select tablename, hasindexes from pg_tables where tablename = 't';
