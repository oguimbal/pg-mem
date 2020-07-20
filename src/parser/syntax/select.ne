@lexer lexer
@include "base.ne"
@include "expr.ne"


select_statement
    -> select_what _ select_from:? {% ([columns, _, table]) => ({columns, table: table, type: 'select'}) %}

select_what -> %kw_select _ expr_list {% get(2) %}
select_from -> %kw_from _ ident {% get(2) %}

expr_list -> expr (_ comma _ expr {% get(3) %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}
