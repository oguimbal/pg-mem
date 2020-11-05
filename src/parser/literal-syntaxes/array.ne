@preprocessor typescript

@{%
import {lexerAny} from './array-lexer';
 %}
@lexer lexerAny
@{%
    const get = (i: number) => (x: any[]) => x[i];
    const last = (x: any[]) => x && x[x.length - 1];
%}

main -> %start_list elements %end_list {% x => x[1] %}

elements -> elt (%comma elt {% last %}):* {% ([head, tail]) => {
    return [head, ...(tail || [])];
} %}

elt -> %value {% x => x[0].value %} | main {% x => x[0] %}