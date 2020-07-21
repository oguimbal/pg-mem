import moo, { Rules } from 'moo';
import { sqlKeywords } from './keywords';

// build keywords
const keywodsMap: any = {};
for (const k of sqlKeywords) {
    keywodsMap['kw_' + k.toLowerCase()] = k;
}
const caseInsensitiveKeywords = (map) => {
    const transform = moo.keywords(map)
    return text => transform(text.toUpperCase())
}


// build lexer
export const lexer = moo.compile({
    word: {
        match: /[a-zA-Z][A-Za-z0-9_\-]*/,
        type: caseInsensitiveKeywords(keywodsMap),
    },
    wordQuoted: {
        match: /"[^"]+"/,
        type: () => 'word',
        // value: x => x.substr(1, x.length - 2),
    },
    string: {
        match: /'(?:[^']|\'\')+'/,
        value: x => JSON.parse('"' + x.substr(1, x.length - 2)
            .replace(/''/g, '\'')
            .replace(/"/g, '\\"') + '"'),
    },
    star: '*',
    comma: ',',
    space: { match: /[\s\t\n\v\f\r]+/, lineBreaks: true },
    int: /[0-9]+/,
    // word: /[a-zA-Z][A-Za-z0-9_\-]*/,
    commentLine: /\-\-.*?$[\s\r\n]*/,
    commentFull: /(?<!\/)\/\*(?:.|[\r\n])+\*\/[\s\r\n]*/,
    lparen: '(',
    rparen: ')',
    lbracket: '[',
    rbracket: ']',
    semicolon: ';',
    dot: '.',
    op_cast: '::',
    op_plus: '+',
    op_minus: /(?<!\-)\-(?!\-)/,
    op_div: /(?<!\/)\/(?!\/)/,
    op_mod: '%',
    op_exp: '^',
    op_additive: {
        // group other additive operators
        match: ['||', '-', '#-', '&&'],
    },
    op_compare: {
        // group other comparison operators
        // ... to add: "IN" and "NOT IN" that are matched by keywords
        match: ['>', '>=', '<', '<=', '@>', '<@', '?', '?|', '?&'],
    },
});