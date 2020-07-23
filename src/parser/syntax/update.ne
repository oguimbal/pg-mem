@lexer lexer
@include "base.ne"
@include "expr.ne"
@include "select.ne"


update_statement -> kw_update table_ref_aliased
                    kw_set update_set_list
                    select_where:?
                    (%kw_returning select_expr_list_aliased {% last %}):?
                    {% x => {
                        const where = unwrap(x[4]);
                        const returning = x[5];
                        return {
                            type: 'update',
                            table: unwrap(x[1]),
                            sets: x[3],
                            ...where ? {where} : {},
                            ...returning ? {returning} : {},
                        }
                    } %}

update_set_list -> update_set (comma update_set {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

update_set -> ident %op_eq (expr | %kw_default {% value %}) {% x => ({
    column: unwrap(x[0]),
    value: unwrap(x[2]),
}) %}