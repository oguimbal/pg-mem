@lexer lexerAny
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
    const ret = [];
    for (const _t of [head, ...(tail || [])]) {
        const t = unwrap(_t);
        if (Array.isArray(t)) {
            ret.push(...t);
        } else {
            ret.push(t);
        }
    }
    return ret;
} %}

update_set -> update_set_one | update_set_multiple

update_set_one -> ident %op_eq (expr | %kw_default {% value %}) {% x => ({
    column: unwrap(x[0]),
    value: unwrap(x[2]),
}) %}

update_set_multiple -> collist_paren %op_eq (lparen expr_list_raw rparen {% get(1) %}) {% x => {
    const cols = x[0];
    const exprs = x[2];
    if (cols.length !== exprs.length) {
        throw new Error('number of columns does not match number of values');
    }
    return cols.map((x: any, i: number) => ({
        column: unwrap(x),
        value: unwrap(exprs[i]),
    }))
} %}