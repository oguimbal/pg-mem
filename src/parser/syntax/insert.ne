@lexer lexer
@include "base.ne"
@include "expr.ne"
@include "select.ne"

insert_statement -> (kw_insert %kw_into)
                        table_ref_aliased
                    collist_paren:?
                    (kw_values insert_values {% last %}):?
                    (select_statement | select_statement_paren):?
                    (%kw_returning select_expr_list_aliased {% last %}):?
                    {% x => {
                        const columns = x[2];
                        const values = x[3];
                        const select = unwrap(x[4]);
                        const returning = x[5];
                        return {
                            type: 'insert',
                            into: unwrap(x[1]),
                            ...columns ? { columns } : {},
                            ...values ? { values } : {},
                            ...select ? { select } : {},
                            ...returning ? { returning } : {},
                        }
                    } %}


insert_values -> insert_value (comma insert_value {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

insert_value -> lparen insert_expr_list_raw rparen {% get(1) %}


insert_single_value -> (expr_or_select | %kw_default {% () => 'default' %}) {% unwrap %}
insert_expr_list_raw -> insert_single_value (comma insert_single_value {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}