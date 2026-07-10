-- @case: to_tsvector simple config keeps all tokens
-- @expect: [{"v":"'brown':3 'foxes':4 'quick':2 'the':1"}]
select to_tsvector('simple', 'The Quick Brown Foxes') as v;

-- @case: to_tsvector english stems and drops stopwords
-- @expect: [{"v":"'brown':3 'fox':4 'jump':5 'quick':2 'run':6"}]
select to_tsvector('english', 'The Quick Brown Foxes jumping running') as v;

-- @case: to_tsvector default config is english
-- @expect: [{"v":"'dog':3 'run':2"}]
select to_tsvector('the running dogs') as v;

-- @case: to_tsvector lists repeated positions
-- @expect: [{"v":"'cat':2 'the':1,3"}]
select to_tsvector('simple', 'the cat the') as v;

-- @case: plainto_tsquery ANDs stemmed tokens
-- @expect: [{"q":"'jump' & 'fox'"}]
select plainto_tsquery('english', 'jumping foxes') as q;

-- @case: to_tsquery keeps operators
-- @expect: [{"q":"'quick' & 'brown'"}]
select to_tsquery('simple', 'quick & brown') as q;

-- @case: @@ matches with stemming
-- @expect: [{"a":true,"b":false}]
select to_tsvector('english', 'running') @@ to_tsquery('english', 'run') as a,
       to_tsvector('simple', 'running') @@ to_tsquery('simple', 'run') as b;

-- @case: @@ evaluates boolean AND / OR / NOT
-- @expect: [{"a":true,"b":false,"c":true,"d":true}]
select to_tsvector('simple', 'a b c') @@ to_tsquery('simple', 'a & b') as a,
       to_tsvector('simple', 'a b c') @@ to_tsquery('simple', 'a & z') as b,
       to_tsvector('simple', 'a b c') @@ to_tsquery('simple', 'z | b') as c,
       to_tsvector('simple', 'a b c') @@ to_tsquery('simple', '!z') as d;

-- @case: full-text predicate over a table
-- @expect: [{"id":1},{"id":3}]
create table docs (id int, body text);
insert into docs values (1, 'the quick brown fox'), (2, 'lazy dogs sleeping'), (3, 'a fox jumped over');
select id from docs
  where to_tsvector('english', body) @@ plainto_tsquery('english', 'foxes')
  order by id;

-- @case: tsvector column stored and matched
-- @expect: [{"id":1}]
create table d (id int, v tsvector);
insert into d values (1, to_tsvector('simple', 'hello world'));
select id from d where v @@ to_tsquery('simple', 'world');
