@lexer lexer
@include "base.ne"


# https://www.postgresql.org/docs/12/sql-createtable.html
createtable_statement -> %kw_create %kw_table kw_ifnotexists:? word lparen createtable_columnList rparen {% x => ({
    type: 'create table',
    ... !!x[2] ? { ifNotExists: true } : {},
    name: x[3],
    columns: x[5],
}) %}



createtable_columnList -> createtable_column (comma createtable_column {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

createtable_column -> word data_type createtable_column_constraint:? {% x => ({
    name: x[0],
    dataType: x[1],
    constraint: x[2],
}) %}

# todo handle advanced constraints (see doc)
createtable_column_constraint
    -> %kw_unique kw_not_null:? {% ([_, nn]) => ({ type: 'unique', ...!!nn ? {notNull: !!nn} : {}}) %}
    | %kw_primary kw_key {% () => ({ type: 'primary key' }) %}