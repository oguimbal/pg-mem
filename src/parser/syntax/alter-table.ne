@lexer lexer
@include "base.ne"
@include "expr.ne"

# https://www.postgresql.org/docs/12/sql-altertable.html

altertable_statement -> kw_alter %kw_table kw_ifexists:? table_ref
                        altertable_action {% x => ({
                            type: 'alter table',
                            table: unwrap(x[1]),
                            ... x[2] ? {ifExists: true} : {},
                            table: unwrap(x[3]),
                            change: unwrap(x[4]),
                        }) %}


altertable_action
    -> altertable_rename_table
    | altertable_rename_column
    | altertable_rename_constraint
    | altertable_add_column
    | altertable_drop_column
    | altertable_alter_column


altertable_rename_table -> kw_rename %kw_to word {% x => ({
    type: 'rename',
    to: unwrap(last(x)),
}) %}

altertable_rename_column -> kw_rename %kw_column:? ident %kw_to ident {% x => ({
    type: 'rename column',
    column: unwrap(x[2]),
    to: unwrap(last(x)),
}) %}

altertable_rename_constraint -> kw_rename %kw_constraint ident %kw_to ident {% x => ({
    type: 'rename constraint',
    constraint: unwrap(x[2]),
    to: unwrap(last(x)),
}) %}

altertable_add_column -> kw_add %kw_column:? kw_ifnotexists:? createtable_column {% x => ({
    type: 'add column',
    ... x[2] ? {ifNotExists: true} : {},
    column: unwrap(x[3]),
}) %}


altertable_drop_column -> kw_drop %kw_column:? kw_ifexists:? ident {% x => ({
    type: 'drop column',
    ... x[2] ? {ifExists: true} : {},
    column: unwrap(x[3]),
}) %}


altertable_alter_column
    ->  kw_alter  %kw_column:? ident altercol {% x => ({
        type: 'alter column',
        column: unwrap(x[2]),
        alter: unwrap(x[3])
    }) %}

altercol
    ->  (kw_set kw_data):? kw_type data_type {% x => ({ type: 'set type', dataType: unwrap(last(x)) }) %}
    | kw_set %kw_default expr {% x => ({type: 'set default', default: unwrap(last(x)) }) %}
    | kw_drop %kw_default {% x => ({type: 'drop default' }) %}
    | (kw_set | kw_drop) kw_not_null {% x => ({type: flattenStr(x).join(' ').toLowerCase() }) %}