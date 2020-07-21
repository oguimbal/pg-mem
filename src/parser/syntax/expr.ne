@lexer lexer
@include "base.ne"

# === MACROS
opt_paren[X]
    -> lparen _ $X _ rparen {% x => x[2] %}
    | $X {% ([x]) => x[0] %}

expr_binary[KW, This, Next]
    -> ($This | expr_paren) _ $KW _ ($Next | expr_paren) {% ([left, _, op, __, right]) => ({
                    type: 'binary',
                    left: unwrap(left),
                    right: unwrap(right),
                    op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
                }) %}
    | $Next {% unwrap %}

expr_ternary[KW1, KW2, This, Next]
    -> ($This | expr_paren) _ $KW1 _ ($This | expr_paren) _ $KW2 _ ($Next | expr_paren) {% x => ({
                    type: 'ternary',
                    value: unwrap(x[0]),
                    lo: unwrap(x[4]),
                    hi: unwrap(x[8]),
                    op: (flattenStr(x[2]).join(' ') || '<error>').toUpperCase(),
                }) %}
    | $Next {% unwrap %}

expr_left_unary[KW, This, Next]
    -> $KW _ ($This | expr_paren) {% ([op, _, operand]) => ({ type: 'unary',
                    op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
                    operand: unwrap(operand),
                }) %}
    | $Next  {% unwrap %}




# ======== Operator precedence
#  -> https://www.postgresql.org/docs/12/sql-syntax-lexical.html#SQL-PRECEDENCE
expr -> expr_paren {% unwrap %} | expr_or {% unwrap %}
expr_paren -> lparen _ expr _ rparen {% get(2) %}
expr_or -> expr_binary[%kw_or, expr_or, expr_and]
expr_and -> expr_binary[%kw_and, expr_and, expr_not]
expr_not -> expr_left_unary[%kw_not, expr_not, expr_is]

expr_is
    -> expr_is _ (%kw_isnull | %kw_is __ %kw_null) {% x => ({ type: 'unary', op: 'IS NULL', operand: unwrap(x[0]) }) %}
    | expr_is _ (%kw_notnull | %kw_is __ kw_not_null)  {% x => ({ type: 'unary', op: 'IS NOT NULL', operand: unwrap(x[0])}) %}
    | expr_is _ %kw_is __ (%kw_not __ {% id %}):? (%kw_true | %kw_false)  {% x => ({
            type: 'unary',
            op: 'IS ' + flattenStr([x[4], x[5]])
                .join(' ')
                .toUpperCase(),
            operand: unwrap(x[0]),
        }) %}
    | expr_compare {% unwrap %}


expr_compare -> expr_binary[%op_compare, expr_compare, expr_range]
expr_range -> expr_ternary[ops_between, %kw_and, expr_range, expr_like]
expr_like -> expr_binary[ops_like, expr_like, expr_add]
expr_add -> expr_binary[(%op_plus | %op_minus | %op_additive), expr_add, expr_mult]
expr_mult -> expr_binary[(%star | %op_div | %op_mod),  expr_mult, expr_exp]
expr_exp -> expr_binary[%op_exp, expr_exp, expr_unary_add]
expr_unary_add -> expr_left_unary[(%op_plus | %op_minus), expr_unary_add, expr_array_index]

expr_array_index
    -> expr_array_index _ %lbracket _ expr_cast _ %rbracket {% x => ({ type: 'arrayIndex', array: x[0], index: x[4] }) %}
    | expr_cast {% unwrap %}

expr_cast
    -> (expr_cast | expr_paren) %op_cast word {% ([operand, _, to]) => ({ type: 'cast', operand: unwrap(operand), to: to.toUpperCase() }) %}
    | expr_dot {% unwrap %}

expr_dot
    -> expr_dot %dot (word | star) {% ([operand, _, member]) => ({ type: 'member', operand: unwrap(operand), member: unwrap(member)}) %}
    | expr_final {% unwrap %}

expr_final
    -> expr_basic
    | expr_primary

expr_basic
    -> expr_call
    | word {% ([value]) => ({ type: 'ref', name: value}) %}

expr_call -> word _ lparen _ expr_list _ rparen {% x => ({
        type: 'call',
        function: x[0].toLowerCase(),
        args: x[4],
    }) %}

expr_primary
    -> float {% ([value]) => ({ type: 'numeric', value: value }) %}
    | int {% ([value]) => ({ type: 'integer', value: value }) %}
    | string {% ([value]) => ({ type: 'string', value: value }) %}
    | %kw_true {% () => ({ type: 'boolean', value: true }) %}
    | %kw_false {% () => ({ type: 'boolean', value: false }) %}
    | star {% ([value]) => ({ type: 'star' }) %}


ops_like ->  (%kw_not __):? (%kw_like | %kw_ilike)
ops_between -> (%kw_not __):? kw_between # {% x => x[0] ? `${x[0][0].value} ${x[1].value}`.toUpperCase() : x[1].value %}

# x,y,z
expr_list -> expr (_ comma _ expr {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}