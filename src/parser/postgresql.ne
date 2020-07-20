@{%
    const {lexer} = require('./lexer.ts');
    function unwrap(e) {
        if (Array.isArray(e) && e.length === 1) {
            return unwrap(e[0]);
        }
        return e;
    }
%}
@lexer lexer
# @preprocessor typescript

main -> expr {% unwrap %}

# Utils
opt_paren[X]
    -> lparen _ $X _ rparen {% x => x[2] %}
    | $X {% ([x]) => x[0] %}

expr_binary[KW, This, Next]
    -> ($This | expr_paren) _ $KW _ ($Next | expr_paren) {% ([left, _, op, __, right]) => ({
                    type: 'binary',
                    left: unwrap(left),
                    right: unwrap(right),
                    op: unwrap(op).value || '<error>'
                }) %}
    | $Next {% unwrap %}

expr_ternary[KW1, KW2, This, Next]
    -> ($This | expr_paren) _ $KW1 _ ($This | expr_paren) _ ($Next | expr_paren)
    | $Next {% unwrap %}

expr_left_unary[KW, This, Next]
    -> $KW _ ($This | expr_paren) {% ([op, _, operand]) => ({ type: 'unary',
                    op: op.value,
                    operand
                }) %}
    | $Next  {% unwrap %}

# Operator precedence
#  -> https://www.postgresql.org/docs/12/sql-syntax-lexical.html#SQL-PRECEDENCE
expr -> expr_parent {% unwrap %} | expr_or {% unwrap %}
expr_paren -> lparen _ expr _ rparen {% x => x[2] %}
expr_or -> expr_binary[%kw_or, expr_or, expr_and]
expr_and -> expr_binary[%kw_or, expr_and, expr_not]
expr_not -> expr_left_unary[%kw_not, expr_not, expr_is]

expr_is
    -> expr_is (%kw_isnull | %kw_notnull) {% unwrap %}
    | expr_is_multiple {% unwrap %}

expr_is_multiple
    -> expr_is_multiple %kw_is __ (%kw_not __):? (%kw_null | %kw_true | %kw_false)
    | expr_compare {% unwrap %}

expr_compare -> expr_binary[%op_compare, expr_compare, expr_range]
expr_range -> expr_ternary[%kw_between, %kw_and, expr_range, expr_like]
expr_like -> expr_binary[ops_like, expr_like, expr_add]
expr_add -> expr_binary[(%op_plus | %op_minus | %op_additive){% unwrap %}, expr_add, expr_mult]
expr_mult -> expr_binary[(%star | %op_div | %op_mod){% unwrap%},  expr_mult, expr_exp]
expr_exp -> expr_binary[%op_exp, expr_exp, expr_unary_add]
expr_unary_add -> expr_left_unary[%op_plusminus, expr_unary_add, expr_array_index]

expr_array_index
    -> expr_array_index _ %lbracket _ expr_cast _ %rbracket {% x => ({ type: 'arrayIndex', array: x[0], index: x[4] }) %}
    | expr_cast {% unwrap %}

expr_cast
    -> (expr_cast | expr_paren) %op_cast %word {% ([operand, _, to]) => ({ type: 'cast', operand: unwrap(operand), to: to.value.toUpperCase() }) %}
    | expr_dot {% unwrap %}

expr_dot
    -> expr_dot %dot word {% ([operand, _, member]) => ({ type: 'member', operand: unwrap(operand), member: unwrap(member)}) %}
    | expr_final {% unwrap %}

expr_final
    -> %word {% ([{value}]) => ({ type: 'ref', name: value}) %}
    | float {% ([value]) => ({ type: 'numeric', value: value }) %}
    | int {% ([value]) => ({ type: 'integer', value: value }) %}


ops_like ->  (%kw_not __):? (%kw_like | %kw_ilike)


# =========== UTILITIES & KEYWORDS
_ -> %space:?
__ -> %space
lparen -> %lparen
rparen -> %rparen
number -> float | int
dot -> %dot {% id %}
float
    -> %int dot %int:? {% args => parseFloat(args.join('')) %}
    | dot %int {% args => parseFloat(args.join('')) %}
int -> %int {% arg => parseInt(arg, 10) %}