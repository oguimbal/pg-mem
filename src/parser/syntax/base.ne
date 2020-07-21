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
ident -> %word {% x => x[0].value %}
word -> %word  {% x => x[0].value %}
comma -> %comma {% id %}
star -> %star {% x => x[0].value %}
string -> %string {% x => x[0].value %}