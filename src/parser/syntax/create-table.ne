@lexer lexerAny
@include "base.ne"


# https://www.postgresql.org/docs/12/sql-createtable.html
createtable_statement -> %kw_create %kw_table kw_ifnotexists:? word lparen createtable_declarationlist rparen {% x => {

    const cols = x[5].filter((v: any) => 'dataType' in v);
    const constraints = x[5].filter((v: any) => !('dataType' in v));

    return {
        type: 'create table',
        ... !!x[2] ? { ifNotExists: true } : {},
        name: x[3],
        columns: cols,
        ...constraints.length ? { constraints } : {},
    }
} %}



createtable_declarationlist -> createtable_declaration (comma createtable_declaration {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

createtable_declaration -> (createtable_constraint | createtable_column) {% unwrap %}

# see "table_constraint" section of doc
createtable_constraint -> (%kw_constraint word {% last %}):? createtable_constraint_def {% x => {
    const name = unwrap(x[0]);
    if (!name) {
        return unwrap(x[1]);
    }
    return {
        constraintName: name,
        ...unwrap(x[1]),
    }
} %}

createtable_constraint_def
    -> kw_not_null {% ()=> ({ type: 'not null' }) %}
    | (%kw_unique | kw_primary_key) lparen createtable_collist rparen {% x => ({
        type: flattenStr(x[0]).join(' ').toLowerCase(),
        columns: x[2],
    }) %}


createtable_collist -> ident (comma ident {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}


createtable_column -> word data_type createtable_column_constraint:? (%kw_default expr {% last %}):? {% x => ({
    name: x[0],
    dataType: x[1],
    ...x[2] ? { constraint: x[2] }: {},
    ...x[3] ? { default: unwrap(x[3]) } : {}
}) %}

# todo handle advanced constraints (see doc)
createtable_column_constraint
    -> %kw_unique kw_not_null:? {% ([_, nn]) => ({ type: 'unique', ...!!nn ? {notNull: !!nn} : {}}) %}
    | kw_primary_key {% () => ({ type: 'primary key' }) %}
    | kw_not_null {% () => ({ type: 'not null' }) %}