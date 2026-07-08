-- @case: create trigger
create table t (id int, updated_at timestamp);
create function touch() returns trigger as $$
begin
    new.updated_at = now();
    return new;
end;
$$ language plpgsql;
create trigger t_touch before update on t for each row execute function touch();
