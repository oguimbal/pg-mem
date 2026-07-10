-- @case: extract year
-- @expect: [{"r":2020}]
select extract(year from date '2020-02-15') as r;

-- @case: date_part
-- @expect: [{"r":2}]
select date_part('month', date '2020-02-15') as r;

-- @case: date_trunc month
create table t (ts timestamp);
insert into t values ('2020-02-15 10:30:00');
select date_trunc('month', ts) as r from t;

-- @case: age between dates
select age(timestamp '2001-04-10', timestamp '1957-06-13') as r;

-- @case: make_date
select make_date(2020, 2, 15) as r;

-- @case: to_char timestamp
-- @expect: [{"r":"2020-02-15"}]
select to_char(timestamp '2020-02-15 10:30:00', 'YYYY-MM-DD') as r;

-- @case: to_date
select to_date('2020-02-15', 'YYYY-MM-DD') as r;

-- @case: interval arithmetic
-- @expect: [{"r":"2020-02-01T00:00:00.000Z"}]
select timestamp '2020-01-01 00:00:00' + interval '1 month' as r;

-- @case: justify_interval
select justify_interval(interval '1 mon -1 hour') as r;
