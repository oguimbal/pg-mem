@lexer lexer
@include "base.ne"


# https://www.postgresql.org/docs/12/sql-createindex.html
createindex_statement -> %kw_create %kw_unique:? kw_index kw_ifnotexists:? word:? %kw_on word lparen createindex_expressions rparen {% x => ({
    type: 'create index',
    ... !!x[1] ? { unique: true } : {},
    ... !!x[3] ? { ifNotExists: true } : {},
    ... !!x[4] ? { indexName: x[4] } : {},
    table: x[6],
    expressions: x[8],
}) %}

createindex_expressions -> createindex_expression (comma createindex_expression {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

createindex_expression -> (expr_basic | expr_paren) (%kw_asc | %kw_desc):? (kw_nulls (kw_first | kw_last) {% last %}):? {% x => ({
    expression: unwrap(x[0]),
    ... !!x[1] ? { order: unwrap(x[1]).value.toLowerCase() } : {},
    ... !!x[2] ? { nulls: unwrap(x[2]) } : {},
}) %}