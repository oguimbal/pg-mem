@lexer lexer
@include "base.ne"
@include "expr.ne"

insert_statement -> (kw_insert __ %kw_into _)
                        table_ref_aliased
                    (_ lparen _ insert_collist _ rparen {% get(3) %}):?
                    (_ kw_values _ insert_values {% last %}):?
                    (_ (select_statement | select_statement_paren) {% last %}):?
                    {% x => {
                        return {
                            type: 'insert',
                            into: unwrap(x[1]),
                            columns: x[2],
                            values: x[3],
                            select: unwrap(x[4]),
                        }
                    } %}

insert_collist -> ident (_ comma _ ident {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}


insert_values -> insert_value (_ comma _ insert_value {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

insert_value -> lparen _ expr_list _ rparen {% get(2) %}