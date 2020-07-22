@lexer lexer
@include "base.ne"

# === MACROS
opt_paren[X]
    -> lparen $X rparen {% x => x[1] %}
    | $X {% ([x]) => x[0] %}

expr_binary[KW, This, Next]
    -> ($This | expr_paren) $KW ($Next | expr_paren) {% ([left, op, right]) => ({
                    type: 'binary',
                    left: unwrap(left),
                    right: unwrap(right),
                    op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
                }) %}
    | $Next {% unwrap %}

expr_ternary[KW1, KW2, This, Next]
    -> ($This | expr_paren) $KW1 ($This | expr_paren) $KW2 ($Next | expr_paren) {% x => ({
                    type: 'ternary',
                    value: unwrap(x[0]),
                    lo: unwrap(x[2]),
                    hi: unwrap(x[4]),
                    op: (flattenStr(x[1]).join(' ') || '<error>').toUpperCase(),
                }) %}
    | $Next {% unwrap %}

expr_left_unary[KW, This, Next]
    -> $KW ($This | expr_paren) {% ([op, operand]) => ({ type: 'unary',
                    op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
                    operand: unwrap(operand),
                }) %}
    | $Next  {% unwrap %}




# ======== Operator precedence
#  -> https://www.postgresql.org/docs/12/sql-syntax-lexical.html#SQL-PRECEDENCE
expr -> expr_paren {% unwrap %} | expr_or {% unwrap %}
expr_paren -> lparen (expr | expr_list_many) rparen {% get(1) %}
expr_or -> expr_binary[%kw_or, expr_or, expr_and]
expr_and -> expr_binary[%kw_and, expr_and, expr_not]
expr_not -> expr_left_unary[%kw_not, expr_not, expr_eq]
expr_eq -> expr_binary[(%op_eq | %op_neq), expr_eq, expr_is]

expr_is
    -> expr_is (%kw_isnull | %kw_is %kw_null) {% x => ({ type: 'unary', op: 'IS NULL', operand: unwrap(x[0]) }) %}
    | expr_is (%kw_notnull | %kw_is kw_not_null)  {% x => ({ type: 'unary', op: 'IS NOT NULL', operand: unwrap(x[0])}) %}
    | expr_is %kw_is %kw_not:? (%kw_true | %kw_false)  {% x => ({
            type: 'unary',
            op: 'IS ' + flattenStr([x[2], x[3]])
                .join(' ')
                .toUpperCase(),
            operand: unwrap(x[0]),
        }) %}
    | expr_compare {% unwrap %}


expr_compare -> expr_binary[%op_compare, expr_compare, expr_range]
expr_range -> expr_ternary[ops_between, %kw_and, expr_range, expr_like]
expr_like -> expr_binary[ops_like, expr_like, expr_in]
expr_in -> expr_binary[ops_in, expr_in, expr_add]
expr_add -> expr_binary[(%op_plus | %op_minus | %op_additive), expr_add, expr_mult]
expr_mult -> expr_binary[(%star | %op_div | %op_mod),  expr_mult, expr_exp]
expr_exp -> expr_binary[%op_exp, expr_exp, expr_unary_add]
expr_unary_add -> expr_left_unary[(%op_plus | %op_minus), expr_unary_add, expr_array_index]

expr_array_index
    -> expr_array_index %lbracket expr_member %rbracket {% x => ({ type: 'arrayIndex', array: x[0], index: x[2] }) %}
    | expr_member {% unwrap %}

expr_member
    -> (expr_member | expr_paren) ops_member (string | int) {% ([operand, op, member]) => ({ type: 'member', operand: unwrap(operand), op, member: unwrap(member)}) %}
    | (expr_member | expr_paren) %op_cast data_type {% ([operand, _, to]) => ({ type: 'cast', operand: unwrap(operand), to }) %}
    | expr_dot {% unwrap %}

expr_dot
    -> word %dot (word | star) {% ([operand, _, member]) => ({ type: 'ref', table: unwrap(operand), name: unwrap(member)}) %}
    | expr_final {% unwrap %}


expr_final
    -> expr_basic
    | expr_primary

expr_basic
    -> expr_call
    | (word | star) {% ([value]) => ({ type: 'ref', name: unwrap(value) }) %}

expr_call -> word lparen expr_list_raw rparen {% x => ({
        type: 'call',
        function: x[0].toLowerCase(),
        args: x[2],
    }) %}

expr_primary
    -> float {% ([value]) => ({ type: 'numeric', value: value }) %}
    | int {% ([value]) => ({ type: 'integer', value: value }) %}
    | string {% ([value]) => ({ type: 'string', value: value }) %}
    | %kw_true {% () => ({ type: 'boolean', value: true }) %}
    | %kw_false {% () => ({ type: 'boolean', value: false }) %}
    | %kw_null {% ([value]) => ({ type: 'null' }) %}


ops_like ->  %kw_not:? (%kw_like | %kw_ilike)
ops_in -> %kw_not:? %kw_in
ops_between -> %kw_not:? kw_between # {% x => x[0] ? `${x[0][0].value} ${x[1].value}`.toUpperCase() : x[1].value %}
ops_member -> (%op_member | %op_membertext) {% x => unwrap(x).value %}

# x,y,z
expr_list_raw -> expr (comma expr {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}
expr_list_raw_many -> expr (comma expr {% last %}):+ {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

expr_list_many -> expr_list_raw_many {% x => ({
    type: 'list',
    expressions: x[0],
}) %}