-- Row-level security. Roles are cluster-global on real postgres, so each case uses
-- distinct role names and drops them up front to stay idempotent across differential runs.

-- @case: select filtered by current_user
-- @offline
-- @expect: [{"id":1},{"id":3}]
drop role if exists rls_a;
create role rls_a nologin;
create table docs (id int, owner text);
insert into docs values (1, 'rls_a'), (2, 'other'), (3, 'rls_a');
alter table docs enable row level security;
grant select on docs to rls_a;
create policy p on docs for select using (owner = current_user);
set role rls_a;
select id from docs order by id;

-- @case: default deny when no policy applies
-- @offline
-- @expect: []
drop role if exists rls_b;
create role rls_b nologin;
create table secret (id int);
insert into secret values (1), (2);
alter table secret enable row level security;
grant select on secret to rls_b;
set role rls_b;
select id from secret order by id;

-- @case: insert violating with check errors
-- @offline
-- @error: violates row-level security policy
drop role if exists rls_c;
create role rls_c nologin;
create table docs (id int, owner text);
alter table docs enable row level security;
grant insert on docs to rls_c;
create policy p on docs for all with check (owner = current_user);
set role rls_c;
insert into docs values (1, 'someone_else');

-- @case: permissive policies are OR-combined
-- @offline
-- @expect: [{"id":1},{"id":2},{"id":3}]
drop role if exists rls_d;
create role rls_d nologin;
create table docs (id int, owner text, flag bool);
insert into docs values (1, 'rls_d', false), (2, 'other', true), (3, 'rls_d', false);
alter table docs enable row level security;
grant select on docs to rls_d;
create policy p1 on docs for select using (owner = current_user);
create policy p2 on docs for select using (flag = true);
set role rls_d;
select id from docs order by id;

-- @case: pg_policies introspection
-- @offline
-- @expect: [{"policyname":"p","cmd":"SELECT","permissive":"PERMISSIVE"}]
drop role if exists rls_e;
create table docs (id int, owner text);
alter table docs enable row level security;
create policy p on docs for select using (owner = current_user);
select policyname, cmd, permissive from pg_policies where tablename = 'docs';
