// Generated automatically by nearley, version 2.19.5
// http://github.com/Hardmath123/nearley
// Bypasses TS6133. Allow declared but unused functions.
// @ts-ignore
function id(d: any[]): any { return d[0]; }
declare var start_list: any;
declare var end_list: any;
declare var comma: any;
declare var value: any;

import {lexerAny} from './array-lexer.ts';
 

    const get = (i: number) => (x: any[]) => x[i];
    const last = (x: any[]) => x && x[x.length - 1];

interface NearleyToken {  value: any;
  [key: string]: any;
};

interface NearleyLexer {
  reset: (chunk: string, info: any) => void;
  next: () => NearleyToken | undefined;
  save: () => any;
  formatError: (token: NearleyToken) => string;
  has: (tokenType: string) => boolean;
};

interface NearleyRule {
  name: string;
  symbols: NearleySymbol[];
  postprocess?: (d: any[], loc?: number, reject?: {}) => any;
};

type NearleySymbol = string | { literal: any } | { test: (token: any) => boolean };

interface Grammar {
  Lexer: NearleyLexer | undefined;
  ParserRules: NearleyRule[];
  ParserStart: string;
};

const grammar: Grammar = {
  Lexer: lexerAny,
  ParserRules: [
    {"name": "main", "symbols": [(lexerAny.has("start_list") ? {type: "start_list"} : start_list), "elements", (lexerAny.has("end_list") ? {type: "end_list"} : end_list)], "postprocess": x => x[1]},
    {"name": "elements$ebnf$1", "symbols": []},
    {"name": "elements$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("comma") ? {type: "comma"} : comma), "elt"], "postprocess": last},
    {"name": "elements$ebnf$1", "symbols": ["elements$ebnf$1", "elements$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "elements", "symbols": ["elt", "elements$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "elt", "symbols": [(lexerAny.has("value") ? {type: "value"} : value)], "postprocess": x => x[0].value},
    {"name": "elt", "symbols": ["main"], "postprocess": x => x[0]}
  ],
  ParserStart: "main",
};

export default grammar;
