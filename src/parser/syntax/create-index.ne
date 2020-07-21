@lexer lexer
@include "base.ne"


# https://www.postgresql.org/docs/12/sql-createindex.html
createindex_statement -> %kw_create (__ %kw_unique):? __ kw_index (__ kw_ifnotexists):? (_ word {% last %}):? _ %kw_on _ word _ lparen _ createindex_expressions _ rparen {% x => ({
    type: 'create index',
    ... !!x[1] ? { unique: true } : {},
    ... !!x[4] ? { ifNotExists: true } : {},
    ... !!x[5] ? { indexName: x[5] } : {},
    table: x[9],
    expressions: x[13],
}) %}

createindex_expressions -> createindex_expression (_ comma _ createindex_expression {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

createindex_expression -> (expr_basic | expr_paren) (_ (%kw_ask | %kw_desc) {% last %}):? (_ kw_nulls __ (kw_first | kw_last) {% last %}):? {% x => ({
    expression: unwrap(x[0]),
    ... !!x[1] ? { order: unwrap(x[1]).value.toLowerCase() } : {},
    ... !!x[2] ? { nulls: unwrap(x[2]) } : {},
}) %}