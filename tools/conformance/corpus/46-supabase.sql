-- Supabase-default primitives

-- @case: gen_random_uuid produces a 36-char uuid
-- @expect: [{"len":36}]
select length(gen_random_uuid()::text) as len;

-- @case: set_config / current_setting round-trip
-- @expect: [{"v":"user-1"}]
select set_config('request.jwt.claims', '{"sub":"user-1"}', true);
select current_setting('request.jwt.claims', true)::json ->> 'sub' as v;

-- @case: uuid primary key with gen_random_uuid() default
-- @expect: [{"c":2}]
create extension if not exists pgcrypto;
create table t (id uuid primary key default gen_random_uuid(), name text);
insert into t(name) values ('a'), ('b');
select count(*)::int as c from t;

-- @case: auth.uid()-style RLS filters rows per user
-- @offline
-- @expect: [{"title":"mine"}]
create schema auth;
create function auth.uid() returns uuid as $$
    select nullif(current_setting('request.jwt.claims', true)::json ->> 'sub', '')::uuid
$$ language sql stable;
create role authenticated;
create table todos (id uuid primary key default gen_random_uuid(), user_id uuid default auth.uid(), title text);
alter table todos enable row level security;
create policy sel on todos for select to authenticated using (user_id = auth.uid());
create policy ins on todos for insert to authenticated with check (user_id = auth.uid());
select set_config('request.jwt.claims', '{"sub":"11111111-1111-1111-1111-111111111111"}', true);
set role authenticated;
insert into todos(title) values ('mine');
select title from todos;
