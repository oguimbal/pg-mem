@lexer lexer
@include "base.ne"
@include "expr.ne"


select_statement
    -> select_what _ select_from:? _ select_where:? {% ([columns, _, from, __, where]) => {
        from = unwrap(from);
        return {
            columns,
            ...from ? { from: [from] } : {},
            where,
            type: 'select',
        }
    } %}

select_statement_paren -> lparen _ select_statement _ rparen {% get(2) %}

# FROM [subject] [alias?]
select_from -> %kw_from _ select_subject {% last %}

# Table name or another select statement wrapped in parens
select_subject -> select_subject_ident | select_subject_select_statement

# Select on tables MAY have an alias
select_subject_ident -> (ident _ dot _ {% id %}):? ident _ select_alias:? {% x => {
    const alias = unwrap(x[3]);
    return {
        type: 'table',
        ...x[0] ? { db: unwrap(x[0]) } : {},
        table: unwrap(x[1]),
        ...alias ? { alias } : {},
    }
} %}

# Selects on subselects MUST have an alias
select_subject_select_statement -> select_statement_paren _ select_alias {% x => ({
    type: 'statement',
    statement: unwrap(x[0]),
    alias: unwrap(x[2])
}) %}

# [AS x] or just [x]
select_alias -> (%kw_as _ ident {% last %}) | ident {% unwrap %}

# SELECT x,y,z
select_what -> %kw_select _ expr_list {% last %}

# WHERE [expr]
select_where -> %kw_where _ expr {% last %}
