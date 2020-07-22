@lexer lexer
@include "base.ne"
@include "expr.ne"

insert_statement -> (kw_insert %kw_into)
                        table_ref_aliased
                    (lparen insert_collist rparen {% get(1) %}):?
                    (kw_values insert_values {% last %}):?
                    (select_statement | select_statement_paren):?
                    {% x => {
                        return {
                            type: 'insert',
                            into: unwrap(x[1]),
                            columns: x[2],
                            values: x[3],
                            select: unwrap(x[4]),
                        }
                    } %}

insert_collist -> ident (comma ident {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}


insert_values -> insert_value (comma insert_value {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

insert_value -> lparen expr_list_raw rparen {% get(1) %}