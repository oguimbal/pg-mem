@lexer lexer
@include "base.ne"
@include "expr.ne"


select_statement
    -> select_what _ select_from:? _ select_where:? {% ([columns, _, from, __, where]) => ({columns, from, where, type: 'select'}) %}

select_statement_paren -> lparen _ select_statement _ rparen {% get(2) %}

# FROM [subject] [alias?]
select_from -> %kw_from _ select_subject _ select_alias:? {% x => ({ subject: unwrap(x[2]), alias: unwrap(x[4]) }) %}

# Table name or another select statement wrapped in parens
select_subject -> ident | select_statement_paren

# [AS x] or just [x]
select_alias -> (%kw_as _ ident {% last %}) | ident

# SELECT x,y,z
select_what -> %kw_select _ expr_list {% last %}

# x,y,z
expr_list -> expr (_ comma _ expr {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

# WHERE [expr]
select_where -> %kw_where _ expr {% last %}