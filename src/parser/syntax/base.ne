@lexer lexer
@{%
    function unwrap(e) {
        if (Array.isArray(e) && e.length === 1) {
            return unwrap(e[0]);
        }
        return e;
    }
    function get(i) {
        return x => x[i];
    }
%}
# @preprocessor typescript

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
ident -> %word {% x => x[0].value %}
word -> %word  {% x => x[0].value %}
comma -> %comma {% id %}
star -> %star {% x => x[0].value %}