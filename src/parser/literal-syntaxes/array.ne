@{% const {lexer} = require('./array-lexer.ts'); %}
@lexer lexer
@{%
    const get = i => x => x[i];
    const last = x => x && x[x.length - 1];
    const value = x => x && x.value;
%}

main -> %start_list elements %end_list {% x => x[1] %}

elements -> elt (%comma elt {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

elt -> %value {% x => x[0].value %} | main {% x => x[0] %}