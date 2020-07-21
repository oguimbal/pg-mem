@lexer lexer
@include "base.ne"


# https://www.postgresql.org/docs/12/sql-createtable.html
createtable_statement -> %kw_create __ %kw_table (__ kw_ifnotexists):? _ word _ lparen _ createtable_columnList _ rparen {% x => ({
    type: 'create table',
    ... !!x[3] ? { ifNotExists: true } : {},
    name: x[5],
    columns: x[9],
}) %}



createtable_columnList -> createtable_column (_ comma _ createtable_column {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

createtable_column -> word _ word _ createtable_column_constraint:? {% x => ({
    name: x[0],
    dataType: x[2],
    constraint: x[4],
}) %}

# todo handle advanced constraints (see doc)
createtable_column_constraint
    -> %kw_unique (__ kw_not_null):? {% ([_, nn]) => ({ type: 'unique', ...!!nn ? {notNull: !!nn} : {}}) %}
    | %kw_primary __ kw_key {% () => ({ type: 'primary key' }) %}