@lexer lexer
@{%
    function unwrap(e) {
        if (Array.isArray(e) && e.length === 1) {
            return unwrap(e[0]);
        }
        return e;
    }
    const get = i => x => x[i];
    const last = x => x && x[x.length - 1];
    const trim = x => x && x.trim();
    const value = x => x && x.value;
    function flatten(e) {
        if (Array.isArray(e)) {
            const ret = [];
            for (const i of e) {
                ret.push(...flatten(i));
            }
            return ret;
        }
        if (!e) {
            return [];
        }
        return [e];
    }
    function flattenStr(e) {
        const fl = flatten(e);
        return fl.filter(x => !!x)
                    .map(x => typeof x === 'string' ? x
                            : 'value' in x ? x.value
                            : x)
                    .filter(x => typeof x === 'string')
                    .map(x => x.trim())
                    .filter(x => !!x);
    }
%}
# @preprocessor typescript


# === Basic constructs
_ -> space:*
__ -> space:+
space -> %space | %commentLine | %commentFull
lparen -> %lparen
rparen -> %rparen
number -> float | int
dot -> %dot {% id %}
float
    -> %int dot %int:? {% args => parseFloat(args.join('')) %}
    | dot %int {% args => parseFloat(args.join('')) %}
int -> %int {% arg => parseInt(arg, 10) %}
comma -> %comma {% id %}
star -> %star {% x => x[0].value %}
string -> %string {% x => x[0].value %}

ident -> word {% unwrap %}
word -> %word  {% x => {
    const val = x[0].value;
    return val[0] === '"' ? val.substr(1, val.length - 2) : val;
} %}
kw_not_null -> %kw_not __ %kw_null

# === Non reserved keywords
# ... which are not in keywords.ts (thus parsed as words)
@{%
 const notReservedKw = (kw) => (x, _, rej) => x[0].value.toLowerCase() === kw ? x[0].value.toLowerCase() : rej
%}
kw_between -> %word {% notReservedKw('between')  %}
kw_if -> %word {% notReservedKw('if')  %}
kw_exists -> %word {% notReservedKw('exists')  %}
kw_key -> %word {% notReservedKw('key')  %}
