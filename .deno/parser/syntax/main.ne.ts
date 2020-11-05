// Generated automatically by nearley, version 2.19.5
// http://github.com/Hardmath123/nearley
// Bypasses TS6133. Allow declared but unused functions.
// @ts-ignore
function id(d: any[]): any { return d[0]; }
declare var lparen: any;
declare var rparen: any;
declare var dot: any;
declare var int: any;
declare var comma: any;
declare var star: any;
declare var string: any;
declare var word: any;
declare var kw_not: any;
declare var kw_null: any;
declare var kw_primary: any;
declare var kw_array: any;
declare var lbracket: any;
declare var rbracket: any;
declare var kw_with: any;
declare var kw_as: any;
declare var kw_current_schema: any;
declare var kw_from: any;
declare var kw_join: any;
declare var kw_on: any;
declare var kw_inner: any;
declare var kw_left: any;
declare var kw_outer: any;
declare var kw_right: any;
declare var kw_full: any;
declare var kw_select: any;
declare var kw_where: any;
declare var kw_group: any;
declare var kw_limit: any;
declare var kw_offset: any;
declare var kw_fetch: any;
declare var kw_order: any;
declare var kw_asc: any;
declare var kw_desc: any;
declare var kw_or: any;
declare var kw_and: any;
declare var kw_not: any;
declare var op_eq: any;
declare var op_neq: any;
declare var kw_isnull: any;
declare var kw_is: any;
declare var kw_null: any;
declare var kw_notnull: any;
declare var kw_true: any;
declare var kw_false: any;
declare var op_compare: any;
declare var op_plus: any;
declare var op_minus: any;
declare var op_additive: any;
declare var star: any;
declare var op_div: any;
declare var op_mod: any;
declare var op_exp: any;
declare var lbracket: any;
declare var rbracket: any;
declare var op_cast: any;
declare var dot: any;
declare var kw_like: any;
declare var kw_ilike: any;
declare var op_like: any;
declare var op_ilike: any;
declare var op_not_like: any;
declare var op_not_ilike: any;
declare var kw_in: any;
declare var op_member: any;
declare var op_membertext: any;
declare var kw_case: any;
declare var kw_end: any;
declare var kw_when: any;
declare var kw_then: any;
declare var kw_else: any;
declare var kw_any: any;
declare var kw_create: any;
declare var kw_table: any;
declare var kw_constraint: any;
declare var kw_unique: any;
declare var kw_default: any;
declare var kw_create: any;
declare var kw_unique: any;
declare var kw_on: any;
declare var kw_asc: any;
declare var kw_desc: any;
declare var kw_into: any;
declare var kw_on: any;
declare var kw_returning: any;
declare var kw_default: any;
declare var kw_do: any;
declare var kw_returning: any;
declare var op_eq: any;
declare var kw_default: any;
declare var kw_table: any;
declare var kw_to: any;
declare var kw_column: any;
declare var kw_constraint: any;
declare var kw_default: any;
declare var kw_primary: any;
declare var kw_foreign: any;
declare var kw_references: any;
declare var kw_on: any;
declare var kw_null: any;
declare var kw_from: any;
declare var kw_returning: any;
declare var kw_table: any;
declare var semicolon: any;

import {lexerAny, LOCATION} from '../lexer.ts';


    function unwrap(e: any[]): any {
        if (Array.isArray(e) && e.length === 1) {
            e = unwrap(e[0]);
        }
        if (Array.isArray(e) && !e.length) {
            return null;
        }
        return e;
    }
    const get = (i: number) => (x: any[]) => x[i];
    const last = (x: any[]) => Array.isArray(x) ? x[x.length - 1] : x;
    const trim = (x: string | null | undefined) => x && x.trim();
    const value = (x: any) => x && x.value;
    function flatten(e: any): any[] {
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
    function flattenStr(e: any): string[] {
        const fl = flatten(e);
        return fl.filter(x => !!x)
                    .map(x => typeof x === 'string' ? x
                            : 'value' in x ? x.value
                            : x)
                    .filter(x => typeof x === 'string')
                    .map(x => x.trim())
                    .filter(x => !!x);
    }


 const notReservedKw = (kw: string) => (x: any[], _: any, rej: any) => {
     const val = typeof x[0] === 'string' ? x[0] : x[0].value;
     const low = val.toLowerCase();
     return low === kw ? low : rej;
 }
 const kw = notReservedKw;
 const anyKw = (...kw: string[]) => (x: any[], _: any, rej: any) => {
     const val = typeof x[0] === 'string' ? x[0] : x[0].value;
     const low = val.toLowerCase();
     return kw.includes(low) ? low : rej;
 }

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
    {"name": "lparen", "symbols": [(lexerAny.has("lparen") ? {type: "lparen"} : lparen)]},
    {"name": "rparen", "symbols": [(lexerAny.has("rparen") ? {type: "rparen"} : rparen)]},
    {"name": "number", "symbols": ["float"]},
    {"name": "number", "symbols": ["int"]},
    {"name": "dot", "symbols": [(lexerAny.has("dot") ? {type: "dot"} : dot)], "postprocess": id},
    {"name": "float$ebnf$1", "symbols": [(lexerAny.has("int") ? {type: "int"} : int)], "postprocess": id},
    {"name": "float$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "float", "symbols": [(lexerAny.has("int") ? {type: "int"} : int), "dot", "float$ebnf$1"], "postprocess": args => parseFloat(args.join(''))},
    {"name": "float", "symbols": ["dot", (lexerAny.has("int") ? {type: "int"} : int)], "postprocess": args => parseFloat(args.join(''))},
    {"name": "int", "symbols": [(lexerAny.has("int") ? {type: "int"} : int)], "postprocess": arg => parseInt(arg as any, 10)},
    {"name": "comma", "symbols": [(lexerAny.has("comma") ? {type: "comma"} : comma)], "postprocess": id},
    {"name": "star", "symbols": [(lexerAny.has("star") ? {type: "star"} : star)], "postprocess": x => x[0].value},
    {"name": "string", "symbols": [(lexerAny.has("string") ? {type: "string"} : string)], "postprocess": x => x[0].value},
    {"name": "ident", "symbols": ["word"], "postprocess": unwrap},
    {"name": "word", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess":  x => {
            const val = x[0].value;
            return val[0] === '"' ? val.substr(1, val.length - 2) : val;
        } },
    {"name": "collist_paren", "symbols": ["lparen", "collist", "rparen"], "postprocess": get(1)},
    {"name": "collist$ebnf$1", "symbols": []},
    {"name": "collist$ebnf$1$subexpression$1", "symbols": ["comma", "ident"], "postprocess": last},
    {"name": "collist$ebnf$1", "symbols": ["collist$ebnf$1", "collist$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "collist", "symbols": ["ident", "collist$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "kw_between", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('between')},
    {"name": "kw_conflict", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('conflict')},
    {"name": "kw_nothing", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('nothing')},
    {"name": "kw_begin", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('begin')},
    {"name": "kw_if", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('if')},
    {"name": "kw_exists", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('exists')},
    {"name": "kw_key", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('key')},
    {"name": "kw_index", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('index')},
    {"name": "kw_nulls", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('nulls')},
    {"name": "kw_first", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('first')},
    {"name": "kw_last", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('last')},
    {"name": "kw_start", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('start')},
    {"name": "kw_commit", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('commit')},
    {"name": "kw_transaction", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('transaction')},
    {"name": "kw_rollback", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('rollback')},
    {"name": "kw_insert", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('insert')},
    {"name": "kw_values", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('values')},
    {"name": "kw_update", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('update')},
    {"name": "kw_set", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('set')},
    {"name": "kw_alter", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('alter')},
    {"name": "kw_rename", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('rename')},
    {"name": "kw_add", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('add')},
    {"name": "kw_drop", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('drop')},
    {"name": "kw_data", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('data')},
    {"name": "kw_type", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('type')},
    {"name": "kw_delete", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('delete')},
    {"name": "kw_cascade", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('cascade')},
    {"name": "kw_no", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('no')},
    {"name": "kw_action", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('action')},
    {"name": "kw_restrict", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('restrict')},
    {"name": "kw_truncate", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('truncate')},
    {"name": "kw_by", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('by')},
    {"name": "kw_row", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('row')},
    {"name": "kw_rows", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('rows')},
    {"name": "kw_next", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": notReservedKw('next')},
    {"name": "kw_ifnotexists", "symbols": ["kw_if", (lexerAny.has("kw_not") ? {type: "kw_not"} : kw_not), "kw_exists"]},
    {"name": "kw_ifexists", "symbols": ["kw_if", "kw_exists"]},
    {"name": "kw_not_null", "symbols": [(lexerAny.has("kw_not") ? {type: "kw_not"} : kw_not), (lexerAny.has("kw_null") ? {type: "kw_null"} : kw_null)]},
    {"name": "kw_primary_key", "symbols": [(lexerAny.has("kw_primary") ? {type: "kw_primary"} : kw_primary), "kw_key"]},
    {"name": "data_type$ebnf$1$subexpression$1", "symbols": ["lparen", "int", "rparen"], "postprocess": get(1)},
    {"name": "data_type$ebnf$1", "symbols": ["data_type$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "data_type$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "data_type$ebnf$2$subexpression$1", "symbols": [(lexerAny.has("kw_array") ? {type: "kw_array"} : kw_array)]},
    {"name": "data_type$ebnf$2$subexpression$1$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("lbracket") ? {type: "lbracket"} : lbracket), (lexerAny.has("rbracket") ? {type: "rbracket"} : rbracket)]},
    {"name": "data_type$ebnf$2$subexpression$1$ebnf$1", "symbols": ["data_type$ebnf$2$subexpression$1$ebnf$1$subexpression$1"]},
    {"name": "data_type$ebnf$2$subexpression$1$ebnf$1$subexpression$2", "symbols": [(lexerAny.has("lbracket") ? {type: "lbracket"} : lbracket), (lexerAny.has("rbracket") ? {type: "rbracket"} : rbracket)]},
    {"name": "data_type$ebnf$2$subexpression$1$ebnf$1", "symbols": ["data_type$ebnf$2$subexpression$1$ebnf$1", "data_type$ebnf$2$subexpression$1$ebnf$1$subexpression$2"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "data_type$ebnf$2$subexpression$1", "symbols": ["data_type$ebnf$2$subexpression$1$ebnf$1"]},
    {"name": "data_type$ebnf$2", "symbols": ["data_type$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "data_type$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "data_type", "symbols": ["data_type_simple", "data_type$ebnf$1", "data_type$ebnf$2"], "postprocess":  x => {
            let asArray = x[2];
            const type = flattenStr(x[0]).join(' ').toLowerCase();
            let ret;
            ret = {
                type,
                ... (typeof x[1] === 'number' && x[1] >= 0 ) ? { length: x[1] } : {},
            };
            if (asArray) {
                if (asArray[0].type === 'kw_array') {
                    asArray = [['array']]
                }
                for (const _ of asArray[0]) {
                    ret = {
                        type: 'array',
                        arrayOf: ret,
                    };
                }
            }
            return ret;
        } },
    {"name": "data_type_simple", "symbols": ["data_type_text"]},
    {"name": "data_type_simple", "symbols": ["data_type_numeric"]},
    {"name": "data_type_simple", "symbols": ["data_type_date"]},
    {"name": "data_type_simple", "symbols": ["word"], "postprocess": anyKw('json', 'jsonb', 'boolean', 'bool', 'money', 'bytea', 'regtype')},
    {"name": "data_type_numeric", "symbols": ["word"], "postprocess": anyKw('smallint', 'int', 'float', 'integer', 'bigint', 'bigint', 'decimal', 'numeric', 'real', 'smallserial', 'serial', 'bigserial')},
    {"name": "data_type_numeric$subexpression$1", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": kw('double')},
    {"name": "data_type_numeric$subexpression$2", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": kw('precision')},
    {"name": "data_type_numeric", "symbols": ["data_type_numeric$subexpression$1", "data_type_numeric$subexpression$2"]},
    {"name": "data_type_text", "symbols": ["word"], "postprocess": anyKw('character', 'varchar', 'char', 'text')},
    {"name": "data_type_text", "symbols": ["word"], "postprocess": kw('character')},
    {"name": "data_type_text$subexpression$1", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": kw('character')},
    {"name": "data_type_text$subexpression$2", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": kw('varying')},
    {"name": "data_type_text", "symbols": ["data_type_text$subexpression$1", "data_type_text$subexpression$2"]},
    {"name": "data_type_date", "symbols": ["word"], "postprocess": kw('date')},
    {"name": "data_type_date", "symbols": ["word"], "postprocess": kw('interval')},
    {"name": "data_type_date", "symbols": ["word"], "postprocess": kw('timestamp')},
    {"name": "data_type_date$subexpression$1", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": anyKw('timestamp', 'time')},
    {"name": "data_type_date$subexpression$2", "symbols": [(lexerAny.has("kw_with") ? {type: "kw_with"} : kw_with)]},
    {"name": "data_type_date$subexpression$2", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": kw('without')},
    {"name": "data_type_date$subexpression$3", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": kw('time')},
    {"name": "data_type_date$subexpression$4", "symbols": [(lexerAny.has("word") ? {type: "word"} : word)], "postprocess": kw('zone')},
    {"name": "data_type_date", "symbols": ["data_type_date$subexpression$1", "data_type_date$subexpression$2", "data_type_date$subexpression$3", "data_type_date$subexpression$4"]},
    {"name": "ident_aliased$subexpression$1", "symbols": [(lexerAny.has("kw_as") ? {type: "kw_as"} : kw_as), "ident"], "postprocess": last},
    {"name": "ident_aliased", "symbols": ["ident_aliased$subexpression$1"]},
    {"name": "ident_aliased", "symbols": ["ident"], "postprocess": unwrap},
    {"name": "table_ref$ebnf$1$subexpression$1", "symbols": ["ident", "dot"], "postprocess": id},
    {"name": "table_ref$ebnf$1", "symbols": ["table_ref$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "table_ref$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "table_ref$subexpression$1", "symbols": ["ident"]},
    {"name": "table_ref$subexpression$1", "symbols": ["current_schema"]},
    {"name": "table_ref", "symbols": ["table_ref$ebnf$1", "table_ref$subexpression$1"], "postprocess":  x => ({
            table: unwrap(x[1]),
            ...x[0] ? { db: unwrap(x[0]) } : {},
        })},
    {"name": "current_schema$ebnf$1$subexpression$1", "symbols": ["lparen", "rparen"]},
    {"name": "current_schema$ebnf$1", "symbols": ["current_schema$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "current_schema$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "current_schema", "symbols": [(lexerAny.has("kw_current_schema") ? {type: "kw_current_schema"} : kw_current_schema), "current_schema$ebnf$1"], "postprocess": () => 'current_schema'},
    {"name": "table_ref_aliased$ebnf$1", "symbols": ["ident_aliased"], "postprocess": id},
    {"name": "table_ref_aliased$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "table_ref_aliased", "symbols": ["table_ref", "table_ref_aliased$ebnf$1"], "postprocess":  x => {
            const alias = unwrap(x[1]);
            return {
                ...unwrap(x[0]),
                ...alias ? { alias } : {},
            }
        } },
    {"name": "select_statement$ebnf$1", "symbols": ["select_from"], "postprocess": id},
    {"name": "select_statement$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_statement$ebnf$2", "symbols": ["select_where"], "postprocess": id},
    {"name": "select_statement$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "select_statement$ebnf$3", "symbols": ["select_groupby"], "postprocess": id},
    {"name": "select_statement$ebnf$3", "symbols": [], "postprocess": () => null},
    {"name": "select_statement$ebnf$4", "symbols": ["select_order_by"], "postprocess": id},
    {"name": "select_statement$ebnf$4", "symbols": [], "postprocess": () => null},
    {"name": "select_statement", "symbols": ["select_what", "select_statement$ebnf$1", "select_statement$ebnf$2", "select_statement$ebnf$3", "select_statement$ebnf$4", "select_limit"], "postprocess":  ([columns, from, where, groupBy, orderBy, limit]) => {
            from = unwrap(from);
            groupBy = groupBy && (groupBy.length === 1 && groupBy[0].type === 'list' ? groupBy[0].expressions : groupBy);
            return {
                columns,
                ...from ? { from: Array.isArray(from) ? from : [from] } : {},
                ...groupBy ? { groupBy } : {},
                ...limit ? { limit } : {},
                ...orderBy ? { orderBy } : {},
                where,
                type: 'select',
            }
        } },
    {"name": "select_statement_paren", "symbols": ["lparen", "select_statement", "rparen"], "postprocess": get(1)},
    {"name": "select_from", "symbols": [(lexerAny.has("kw_from") ? {type: "kw_from"} : kw_from), "select_subject"], "postprocess": last},
    {"name": "select_subject$ebnf$1", "symbols": []},
    {"name": "select_subject$ebnf$1", "symbols": ["select_subject$ebnf$1", "select_table_join"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "select_subject", "symbols": ["select_table_base", "select_subject$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "select_table_base", "symbols": ["table_ref_aliased"], "postprocess": x => ({ type: 'table', ...x[0]})},
    {"name": "select_table_base", "symbols": ["select_subject_select_statement"], "postprocess": unwrap},
    {"name": "select_table_join$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_on") ? {type: "kw_on"} : kw_on), "expr"], "postprocess": last},
    {"name": "select_table_join$ebnf$1", "symbols": ["select_table_join$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "select_table_join$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_table_join", "symbols": ["select_join_op", (lexerAny.has("kw_join") ? {type: "kw_join"} : kw_join), "select_table_base", "select_table_join$ebnf$1"], "postprocess":  x => ({
            ...unwrap(x[2]),
            join: {
                type: flattenStr(x[0]).join(' '),
                on: unwrap(x[3]),
            }
        }) },
    {"name": "select_join_op$subexpression$1$ebnf$1", "symbols": [(lexerAny.has("kw_inner") ? {type: "kw_inner"} : kw_inner)], "postprocess": id},
    {"name": "select_join_op$subexpression$1$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_join_op$subexpression$1", "symbols": ["select_join_op$subexpression$1$ebnf$1"], "postprocess": () => 'INNER JOIN'},
    {"name": "select_join_op", "symbols": ["select_join_op$subexpression$1"]},
    {"name": "select_join_op$subexpression$2$ebnf$1", "symbols": [(lexerAny.has("kw_outer") ? {type: "kw_outer"} : kw_outer)], "postprocess": id},
    {"name": "select_join_op$subexpression$2$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_join_op$subexpression$2", "symbols": [(lexerAny.has("kw_left") ? {type: "kw_left"} : kw_left), "select_join_op$subexpression$2$ebnf$1"], "postprocess": () => 'LEFT JOIN'},
    {"name": "select_join_op", "symbols": ["select_join_op$subexpression$2"]},
    {"name": "select_join_op$subexpression$3$ebnf$1", "symbols": [(lexerAny.has("kw_outer") ? {type: "kw_outer"} : kw_outer)], "postprocess": id},
    {"name": "select_join_op$subexpression$3$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_join_op$subexpression$3", "symbols": [(lexerAny.has("kw_right") ? {type: "kw_right"} : kw_right), "select_join_op$subexpression$3$ebnf$1"], "postprocess": () => 'RIGHT JOIN'},
    {"name": "select_join_op", "symbols": ["select_join_op$subexpression$3"]},
    {"name": "select_join_op$subexpression$4$ebnf$1", "symbols": [(lexerAny.has("kw_outer") ? {type: "kw_outer"} : kw_outer)], "postprocess": id},
    {"name": "select_join_op$subexpression$4$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_join_op$subexpression$4", "symbols": [(lexerAny.has("kw_full") ? {type: "kw_full"} : kw_full), "select_join_op$subexpression$4$ebnf$1"], "postprocess": () => 'FULL JOIN'},
    {"name": "select_join_op", "symbols": ["select_join_op$subexpression$4"]},
    {"name": "select_subject_select_statement", "symbols": ["select_statement_paren", "ident_aliased"], "postprocess":  x => ({
            type: 'statement',
            statement: unwrap(x[0]),
            alias: unwrap(x[1])
        }) },
    {"name": "select_what", "symbols": [(lexerAny.has("kw_select") ? {type: "kw_select"} : kw_select), "select_expr_list_aliased"], "postprocess": last},
    {"name": "select_expr_list_aliased$ebnf$1", "symbols": []},
    {"name": "select_expr_list_aliased$ebnf$1$subexpression$1", "symbols": ["comma", "select_expr_list_item"], "postprocess": last},
    {"name": "select_expr_list_aliased$ebnf$1", "symbols": ["select_expr_list_aliased$ebnf$1", "select_expr_list_aliased$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "select_expr_list_aliased", "symbols": ["select_expr_list_item", "select_expr_list_aliased$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "select_expr_list_item$ebnf$1", "symbols": ["ident_aliased"], "postprocess": id},
    {"name": "select_expr_list_item$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_expr_list_item", "symbols": ["expr", "select_expr_list_item$ebnf$1"], "postprocess":  x => ({
            expr: x[0],
            ...x[1] ? {alias: unwrap(x[1]) } : {},
        }) },
    {"name": "select_where", "symbols": [(lexerAny.has("kw_where") ? {type: "kw_where"} : kw_where), "expr"], "postprocess": last},
    {"name": "select_groupby", "symbols": [(lexerAny.has("kw_group") ? {type: "kw_group"} : kw_group), "kw_by", "expr_list_raw"], "postprocess": last},
    {"name": "select_limit$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_limit") ? {type: "kw_limit"} : kw_limit), "int"], "postprocess": last},
    {"name": "select_limit$ebnf$1", "symbols": ["select_limit$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "select_limit$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_limit$ebnf$2$subexpression$1$ebnf$1$subexpression$1", "symbols": ["kw_row"]},
    {"name": "select_limit$ebnf$2$subexpression$1$ebnf$1$subexpression$1", "symbols": ["kw_rows"]},
    {"name": "select_limit$ebnf$2$subexpression$1$ebnf$1", "symbols": ["select_limit$ebnf$2$subexpression$1$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "select_limit$ebnf$2$subexpression$1$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_limit$ebnf$2$subexpression$1", "symbols": [(lexerAny.has("kw_offset") ? {type: "kw_offset"} : kw_offset), "int", "select_limit$ebnf$2$subexpression$1$ebnf$1"], "postprocess": get(1)},
    {"name": "select_limit$ebnf$2", "symbols": ["select_limit$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "select_limit$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$1$subexpression$1", "symbols": ["kw_first"]},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$1$subexpression$1", "symbols": ["kw_next"]},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$1", "symbols": ["select_limit$ebnf$3$subexpression$1$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$2$subexpression$1", "symbols": ["kw_row"]},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$2$subexpression$1", "symbols": ["kw_rows"]},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$2", "symbols": ["select_limit$ebnf$3$subexpression$1$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "select_limit$ebnf$3$subexpression$1$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "select_limit$ebnf$3$subexpression$1", "symbols": [(lexerAny.has("kw_fetch") ? {type: "kw_fetch"} : kw_fetch), "select_limit$ebnf$3$subexpression$1$ebnf$1", "int", "select_limit$ebnf$3$subexpression$1$ebnf$2"], "postprocess": get(2)},
    {"name": "select_limit$ebnf$3", "symbols": ["select_limit$ebnf$3$subexpression$1"], "postprocess": id},
    {"name": "select_limit$ebnf$3", "symbols": [], "postprocess": () => null},
    {"name": "select_limit", "symbols": ["select_limit$ebnf$1", "select_limit$ebnf$2", "select_limit$ebnf$3"], "postprocess":  ([limit1, offset, limit2], _, rej) => {
            if (typeof limit1 === 'number' && typeof limit2 === 'number') {
                return rej;
            }
            if (typeof limit1 !== 'number' && typeof limit2 !== 'number' && typeof offset !== 'number') {
                return null;
            }
            const limit = typeof limit1 === 'number' ? limit1 : limit2;
            return {
                ...typeof limit === 'number' ? {limit}: {},
                ...offset ? {offset} : {},
            }
        }},
    {"name": "select_order_by$subexpression$1", "symbols": [(lexerAny.has("kw_order") ? {type: "kw_order"} : kw_order), "kw_by"]},
    {"name": "select_order_by$ebnf$1", "symbols": []},
    {"name": "select_order_by$ebnf$1$subexpression$1", "symbols": ["comma", "select_order_by_expr"], "postprocess": last},
    {"name": "select_order_by$ebnf$1", "symbols": ["select_order_by$ebnf$1", "select_order_by$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "select_order_by", "symbols": ["select_order_by$subexpression$1", "select_order_by_expr", "select_order_by$ebnf$1"], "postprocess":  ([_, head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "select_order_by_expr$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_asc") ? {type: "kw_asc"} : kw_asc)]},
    {"name": "select_order_by_expr$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_desc") ? {type: "kw_desc"} : kw_desc)]},
    {"name": "select_order_by_expr$ebnf$1", "symbols": ["select_order_by_expr$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "select_order_by_expr$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "select_order_by_expr", "symbols": ["expr", "select_order_by_expr$ebnf$1"], "postprocess":  ([by, order]) => ({
            by,
            order: flattenStr(order).join('').toUpperCase() || 'ASC',
        }) },
    {"name": "expr", "symbols": ["expr_paren"], "postprocess": unwrap},
    {"name": "expr", "symbols": ["expr_or"], "postprocess": unwrap},
    {"name": "expr_paren$subexpression$1", "symbols": ["expr_or_select"]},
    {"name": "expr_paren$subexpression$1", "symbols": ["expr_list_many"]},
    {"name": "expr_paren", "symbols": ["lparen", "expr_paren$subexpression$1", "rparen"], "postprocess": get(1)},
    {"name": "expr_or$macrocall$2", "symbols": [(lexerAny.has("kw_or") ? {type: "kw_or"} : kw_or)]},
    {"name": "expr_or$macrocall$3", "symbols": ["expr_or"]},
    {"name": "expr_or$macrocall$4", "symbols": ["expr_and"]},
    {"name": "expr_or$macrocall$1$subexpression$1", "symbols": ["expr_or$macrocall$3"]},
    {"name": "expr_or$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_or$macrocall$1$subexpression$2", "symbols": ["expr_or$macrocall$4"]},
    {"name": "expr_or$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_or$macrocall$1", "symbols": ["expr_or$macrocall$1$subexpression$1", "expr_or$macrocall$2", "expr_or$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_or$macrocall$1", "symbols": ["expr_or$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_or", "symbols": ["expr_or$macrocall$1"]},
    {"name": "expr_and$macrocall$2", "symbols": [(lexerAny.has("kw_and") ? {type: "kw_and"} : kw_and)]},
    {"name": "expr_and$macrocall$3", "symbols": ["expr_and"]},
    {"name": "expr_and$macrocall$4", "symbols": ["expr_not"]},
    {"name": "expr_and$macrocall$1$subexpression$1", "symbols": ["expr_and$macrocall$3"]},
    {"name": "expr_and$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_and$macrocall$1$subexpression$2", "symbols": ["expr_and$macrocall$4"]},
    {"name": "expr_and$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_and$macrocall$1", "symbols": ["expr_and$macrocall$1$subexpression$1", "expr_and$macrocall$2", "expr_and$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_and$macrocall$1", "symbols": ["expr_and$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_and", "symbols": ["expr_and$macrocall$1"]},
    {"name": "expr_not$macrocall$2", "symbols": [(lexerAny.has("kw_not") ? {type: "kw_not"} : kw_not)]},
    {"name": "expr_not$macrocall$3", "symbols": ["expr_not"]},
    {"name": "expr_not$macrocall$4", "symbols": ["expr_eq"]},
    {"name": "expr_not$macrocall$1$subexpression$1", "symbols": ["expr_not$macrocall$3"]},
    {"name": "expr_not$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_not$macrocall$1", "symbols": ["expr_not$macrocall$2", "expr_not$macrocall$1$subexpression$1"], "postprocess":  ([op, operand]) => ({ type: 'unary',
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
            operand: unwrap(operand),
        }) },
    {"name": "expr_not$macrocall$1", "symbols": ["expr_not$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_not", "symbols": ["expr_not$macrocall$1"]},
    {"name": "expr_eq$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_eq") ? {type: "op_eq"} : op_eq)]},
    {"name": "expr_eq$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_neq") ? {type: "op_neq"} : op_neq)]},
    {"name": "expr_eq$macrocall$2", "symbols": ["expr_eq$macrocall$2$subexpression$1"]},
    {"name": "expr_eq$macrocall$3", "symbols": ["expr_eq"]},
    {"name": "expr_eq$macrocall$4", "symbols": ["expr_is"]},
    {"name": "expr_eq$macrocall$1$subexpression$1", "symbols": ["expr_eq$macrocall$3"]},
    {"name": "expr_eq$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_eq$macrocall$1$subexpression$2", "symbols": ["expr_eq$macrocall$4"]},
    {"name": "expr_eq$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_eq$macrocall$1", "symbols": ["expr_eq$macrocall$1$subexpression$1", "expr_eq$macrocall$2", "expr_eq$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_eq$macrocall$1", "symbols": ["expr_eq$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_eq", "symbols": ["expr_eq$macrocall$1"]},
    {"name": "expr_is$subexpression$1", "symbols": [(lexerAny.has("kw_isnull") ? {type: "kw_isnull"} : kw_isnull)]},
    {"name": "expr_is$subexpression$1", "symbols": [(lexerAny.has("kw_is") ? {type: "kw_is"} : kw_is), (lexerAny.has("kw_null") ? {type: "kw_null"} : kw_null)]},
    {"name": "expr_is", "symbols": ["expr_is", "expr_is$subexpression$1"], "postprocess": x => ({ type: 'unary', op: 'IS NULL', operand: unwrap(x[0]) })},
    {"name": "expr_is$subexpression$2", "symbols": [(lexerAny.has("kw_notnull") ? {type: "kw_notnull"} : kw_notnull)]},
    {"name": "expr_is$subexpression$2", "symbols": [(lexerAny.has("kw_is") ? {type: "kw_is"} : kw_is), "kw_not_null"]},
    {"name": "expr_is", "symbols": ["expr_is", "expr_is$subexpression$2"], "postprocess": x => ({ type: 'unary', op: 'IS NOT NULL', operand: unwrap(x[0])})},
    {"name": "expr_is$ebnf$1", "symbols": [(lexerAny.has("kw_not") ? {type: "kw_not"} : kw_not)], "postprocess": id},
    {"name": "expr_is$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "expr_is$subexpression$3", "symbols": [(lexerAny.has("kw_true") ? {type: "kw_true"} : kw_true)]},
    {"name": "expr_is$subexpression$3", "symbols": [(lexerAny.has("kw_false") ? {type: "kw_false"} : kw_false)]},
    {"name": "expr_is", "symbols": ["expr_is", (lexerAny.has("kw_is") ? {type: "kw_is"} : kw_is), "expr_is$ebnf$1", "expr_is$subexpression$3"], "postprocess":  x => ({
            type: 'unary',
            op: 'IS ' + flattenStr([x[2], x[3]])
                .join(' ')
                .toUpperCase(),
            operand: unwrap(x[0]),
        }) },
    {"name": "expr_is", "symbols": ["expr_compare"], "postprocess": unwrap},
    {"name": "expr_compare$macrocall$2", "symbols": [(lexerAny.has("op_compare") ? {type: "op_compare"} : op_compare)]},
    {"name": "expr_compare$macrocall$3", "symbols": ["expr_compare"]},
    {"name": "expr_compare$macrocall$4", "symbols": ["expr_range"]},
    {"name": "expr_compare$macrocall$1$subexpression$1", "symbols": ["expr_compare$macrocall$3"]},
    {"name": "expr_compare$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_compare$macrocall$1$subexpression$2", "symbols": ["expr_compare$macrocall$4"]},
    {"name": "expr_compare$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_compare$macrocall$1", "symbols": ["expr_compare$macrocall$1$subexpression$1", "expr_compare$macrocall$2", "expr_compare$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_compare$macrocall$1", "symbols": ["expr_compare$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_compare", "symbols": ["expr_compare$macrocall$1"]},
    {"name": "expr_range$macrocall$2", "symbols": ["ops_between"]},
    {"name": "expr_range$macrocall$3", "symbols": [(lexerAny.has("kw_and") ? {type: "kw_and"} : kw_and)]},
    {"name": "expr_range$macrocall$4", "symbols": ["expr_range"]},
    {"name": "expr_range$macrocall$5", "symbols": ["expr_like"]},
    {"name": "expr_range$macrocall$1$subexpression$1", "symbols": ["expr_range$macrocall$4"]},
    {"name": "expr_range$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_range$macrocall$1$subexpression$2", "symbols": ["expr_range$macrocall$4"]},
    {"name": "expr_range$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_range$macrocall$1$subexpression$3", "symbols": ["expr_range$macrocall$5"]},
    {"name": "expr_range$macrocall$1$subexpression$3", "symbols": ["expr_paren"]},
    {"name": "expr_range$macrocall$1", "symbols": ["expr_range$macrocall$1$subexpression$1", "expr_range$macrocall$2", "expr_range$macrocall$1$subexpression$2", "expr_range$macrocall$3", "expr_range$macrocall$1$subexpression$3"], "postprocess":  x => ({
            type: 'ternary',
            value: unwrap(x[0]),
            lo: unwrap(x[2]),
            hi: unwrap(x[4]),
            op: (flattenStr(x[1]).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_range$macrocall$1", "symbols": ["expr_range$macrocall$5"], "postprocess": unwrap},
    {"name": "expr_range", "symbols": ["expr_range$macrocall$1"]},
    {"name": "expr_like$macrocall$2", "symbols": ["ops_like"]},
    {"name": "expr_like$macrocall$3", "symbols": ["expr_like"]},
    {"name": "expr_like$macrocall$4", "symbols": ["expr_in"]},
    {"name": "expr_like$macrocall$1$subexpression$1", "symbols": ["expr_like$macrocall$3"]},
    {"name": "expr_like$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_like$macrocall$1$subexpression$2", "symbols": ["expr_like$macrocall$4"]},
    {"name": "expr_like$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_like$macrocall$1", "symbols": ["expr_like$macrocall$1$subexpression$1", "expr_like$macrocall$2", "expr_like$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_like$macrocall$1", "symbols": ["expr_like$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_like", "symbols": ["expr_like$macrocall$1"]},
    {"name": "expr_in$macrocall$2", "symbols": ["ops_in"]},
    {"name": "expr_in$macrocall$3", "symbols": ["expr_in"]},
    {"name": "expr_in$macrocall$4", "symbols": ["expr_add"]},
    {"name": "expr_in$macrocall$1$subexpression$1", "symbols": ["expr_in$macrocall$3"]},
    {"name": "expr_in$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_in$macrocall$1$subexpression$2", "symbols": ["expr_in$macrocall$4"]},
    {"name": "expr_in$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_in$macrocall$1", "symbols": ["expr_in$macrocall$1$subexpression$1", "expr_in$macrocall$2", "expr_in$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_in$macrocall$1", "symbols": ["expr_in$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_in", "symbols": ["expr_in$macrocall$1"]},
    {"name": "expr_add$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_plus") ? {type: "op_plus"} : op_plus)]},
    {"name": "expr_add$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_minus") ? {type: "op_minus"} : op_minus)]},
    {"name": "expr_add$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_additive") ? {type: "op_additive"} : op_additive)]},
    {"name": "expr_add$macrocall$2", "symbols": ["expr_add$macrocall$2$subexpression$1"]},
    {"name": "expr_add$macrocall$3", "symbols": ["expr_add"]},
    {"name": "expr_add$macrocall$4", "symbols": ["expr_mult"]},
    {"name": "expr_add$macrocall$1$subexpression$1", "symbols": ["expr_add$macrocall$3"]},
    {"name": "expr_add$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_add$macrocall$1$subexpression$2", "symbols": ["expr_add$macrocall$4"]},
    {"name": "expr_add$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_add$macrocall$1", "symbols": ["expr_add$macrocall$1$subexpression$1", "expr_add$macrocall$2", "expr_add$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_add$macrocall$1", "symbols": ["expr_add$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_add", "symbols": ["expr_add$macrocall$1"]},
    {"name": "expr_mult$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("star") ? {type: "star"} : star)]},
    {"name": "expr_mult$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_div") ? {type: "op_div"} : op_div)]},
    {"name": "expr_mult$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_mod") ? {type: "op_mod"} : op_mod)]},
    {"name": "expr_mult$macrocall$2", "symbols": ["expr_mult$macrocall$2$subexpression$1"]},
    {"name": "expr_mult$macrocall$3", "symbols": ["expr_mult"]},
    {"name": "expr_mult$macrocall$4", "symbols": ["expr_exp"]},
    {"name": "expr_mult$macrocall$1$subexpression$1", "symbols": ["expr_mult$macrocall$3"]},
    {"name": "expr_mult$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_mult$macrocall$1$subexpression$2", "symbols": ["expr_mult$macrocall$4"]},
    {"name": "expr_mult$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_mult$macrocall$1", "symbols": ["expr_mult$macrocall$1$subexpression$1", "expr_mult$macrocall$2", "expr_mult$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_mult$macrocall$1", "symbols": ["expr_mult$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_mult", "symbols": ["expr_mult$macrocall$1"]},
    {"name": "expr_exp$macrocall$2", "symbols": [(lexerAny.has("op_exp") ? {type: "op_exp"} : op_exp)]},
    {"name": "expr_exp$macrocall$3", "symbols": ["expr_exp"]},
    {"name": "expr_exp$macrocall$4", "symbols": ["expr_unary_add"]},
    {"name": "expr_exp$macrocall$1$subexpression$1", "symbols": ["expr_exp$macrocall$3"]},
    {"name": "expr_exp$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_exp$macrocall$1$subexpression$2", "symbols": ["expr_exp$macrocall$4"]},
    {"name": "expr_exp$macrocall$1$subexpression$2", "symbols": ["expr_paren"]},
    {"name": "expr_exp$macrocall$1", "symbols": ["expr_exp$macrocall$1$subexpression$1", "expr_exp$macrocall$2", "expr_exp$macrocall$1$subexpression$2"], "postprocess":  ([left, op, right]) => ({
            type: 'binary',
            left: unwrap(left),
            right: unwrap(right),
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
        }) },
    {"name": "expr_exp$macrocall$1", "symbols": ["expr_exp$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_exp", "symbols": ["expr_exp$macrocall$1"]},
    {"name": "expr_unary_add$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_plus") ? {type: "op_plus"} : op_plus)]},
    {"name": "expr_unary_add$macrocall$2$subexpression$1", "symbols": [(lexerAny.has("op_minus") ? {type: "op_minus"} : op_minus)]},
    {"name": "expr_unary_add$macrocall$2", "symbols": ["expr_unary_add$macrocall$2$subexpression$1"]},
    {"name": "expr_unary_add$macrocall$3", "symbols": ["expr_unary_add"]},
    {"name": "expr_unary_add$macrocall$4", "symbols": ["expr_array_index"]},
    {"name": "expr_unary_add$macrocall$1$subexpression$1", "symbols": ["expr_unary_add$macrocall$3"]},
    {"name": "expr_unary_add$macrocall$1$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_unary_add$macrocall$1", "symbols": ["expr_unary_add$macrocall$2", "expr_unary_add$macrocall$1$subexpression$1"], "postprocess":  ([op, operand]) => ({ type: 'unary',
            op: (flattenStr(op).join(' ') || '<error>').toUpperCase(),
            operand: unwrap(operand),
        }) },
    {"name": "expr_unary_add$macrocall$1", "symbols": ["expr_unary_add$macrocall$4"], "postprocess": unwrap},
    {"name": "expr_unary_add", "symbols": ["expr_unary_add$macrocall$1"]},
    {"name": "expr_array_index", "symbols": ["expr_array_index", (lexerAny.has("lbracket") ? {type: "lbracket"} : lbracket), "expr_member", (lexerAny.has("rbracket") ? {type: "rbracket"} : rbracket)], "postprocess": x => ({ type: 'arrayIndex', array: x[0], index: x[2] })},
    {"name": "expr_array_index", "symbols": ["expr_member"], "postprocess": unwrap},
    {"name": "expr_member$subexpression$1", "symbols": ["expr_member"]},
    {"name": "expr_member$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "expr_member$subexpression$2", "symbols": ["string"]},
    {"name": "expr_member$subexpression$2", "symbols": ["int"]},
    {"name": "expr_member", "symbols": ["expr_member$subexpression$1", "ops_member", "expr_member$subexpression$2"], "postprocess": ([operand, op, member]) => ({ type: 'member', operand: unwrap(operand), op, member: unwrap(member)})},
    {"name": "expr_member$subexpression$3", "symbols": ["expr_member"]},
    {"name": "expr_member$subexpression$3", "symbols": ["expr_paren"]},
    {"name": "expr_member", "symbols": ["expr_member$subexpression$3", (lexerAny.has("op_cast") ? {type: "op_cast"} : op_cast), "data_type"], "postprocess": ([operand, _, to]) => ({ type: 'cast', operand: unwrap(operand), to })},
    {"name": "expr_member", "symbols": ["expr_dot"], "postprocess": unwrap},
    {"name": "expr_dot$subexpression$1", "symbols": ["word"]},
    {"name": "expr_dot$subexpression$1", "symbols": ["star"]},
    {"name": "expr_dot", "symbols": ["word", (lexerAny.has("dot") ? {type: "dot"} : dot), "expr_dot$subexpression$1"], "postprocess": ([operand, _, member]) => ({ type: 'ref', table: unwrap(operand), name: unwrap(member)})},
    {"name": "expr_dot", "symbols": ["expr_final"], "postprocess": unwrap},
    {"name": "expr_final", "symbols": ["expr_basic"]},
    {"name": "expr_final", "symbols": ["expr_primary"]},
    {"name": "expr_basic", "symbols": ["expr_call"]},
    {"name": "expr_basic", "symbols": ["expr_case"]},
    {"name": "expr_basic", "symbols": ["current_schema"], "postprocess": () => ({ type: 'call', function: 'current_schema', args: [] })},
    {"name": "expr_basic$subexpression$1", "symbols": ["word"]},
    {"name": "expr_basic$subexpression$1", "symbols": ["star"]},
    {"name": "expr_basic", "symbols": ["expr_basic$subexpression$1"], "postprocess": ([value]) => ({ type: 'ref', name: unwrap(value) })},
    {"name": "expr_call$ebnf$1", "symbols": ["expr_list_raw"], "postprocess": id},
    {"name": "expr_call$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "expr_call", "symbols": ["expr_fn_name", "lparen", "expr_call$ebnf$1", "rparen"], "postprocess":  x => ({
            type: 'call',
            function: flattenStr(x[0]).join('').toLowerCase(),
            args: x[2] || [],
        }) },
    {"name": "expr_primary", "symbols": ["float"], "postprocess": ([value]) => ({ type: 'numeric', value: value })},
    {"name": "expr_primary", "symbols": ["int"], "postprocess": ([value]) => ({ type: 'integer', value: value })},
    {"name": "expr_primary", "symbols": ["string"], "postprocess": ([value]) => ({ type: 'string', value: value })},
    {"name": "expr_primary", "symbols": [(lexerAny.has("kw_true") ? {type: "kw_true"} : kw_true)], "postprocess": () => ({ type: 'boolean', value: true })},
    {"name": "expr_primary", "symbols": [(lexerAny.has("kw_false") ? {type: "kw_false"} : kw_false)], "postprocess": () => ({ type: 'boolean', value: false })},
    {"name": "expr_primary", "symbols": [(lexerAny.has("kw_null") ? {type: "kw_null"} : kw_null)], "postprocess": ([value]) => ({ type: 'null' })},
    {"name": "ops_like", "symbols": ["ops_like_keywors"]},
    {"name": "ops_like", "symbols": ["ops_like_operators"]},
    {"name": "ops_like_keywors$ebnf$1", "symbols": [(lexerAny.has("kw_not") ? {type: "kw_not"} : kw_not)], "postprocess": id},
    {"name": "ops_like_keywors$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "ops_like_keywors$subexpression$1", "symbols": [(lexerAny.has("kw_like") ? {type: "kw_like"} : kw_like)]},
    {"name": "ops_like_keywors$subexpression$1", "symbols": [(lexerAny.has("kw_ilike") ? {type: "kw_ilike"} : kw_ilike)]},
    {"name": "ops_like_keywors", "symbols": ["ops_like_keywors$ebnf$1", "ops_like_keywors$subexpression$1"]},
    {"name": "ops_like_operators$subexpression$1", "symbols": [(lexerAny.has("op_like") ? {type: "op_like"} : op_like)], "postprocess": () => 'LIKE'},
    {"name": "ops_like_operators", "symbols": ["ops_like_operators$subexpression$1"]},
    {"name": "ops_like_operators$subexpression$2", "symbols": [(lexerAny.has("op_ilike") ? {type: "op_ilike"} : op_ilike)], "postprocess": () => 'ILIKE'},
    {"name": "ops_like_operators", "symbols": ["ops_like_operators$subexpression$2"]},
    {"name": "ops_like_operators$subexpression$3", "symbols": [(lexerAny.has("op_not_like") ? {type: "op_not_like"} : op_not_like)], "postprocess": () => 'NOT LIKE'},
    {"name": "ops_like_operators", "symbols": ["ops_like_operators$subexpression$3"]},
    {"name": "ops_like_operators$subexpression$4", "symbols": [(lexerAny.has("op_not_ilike") ? {type: "op_not_ilike"} : op_not_ilike)], "postprocess": () => 'NOT ILIKE'},
    {"name": "ops_like_operators", "symbols": ["ops_like_operators$subexpression$4"]},
    {"name": "ops_in$ebnf$1", "symbols": [(lexerAny.has("kw_not") ? {type: "kw_not"} : kw_not)], "postprocess": id},
    {"name": "ops_in$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "ops_in", "symbols": ["ops_in$ebnf$1", (lexerAny.has("kw_in") ? {type: "kw_in"} : kw_in)]},
    {"name": "ops_between$ebnf$1", "symbols": [(lexerAny.has("kw_not") ? {type: "kw_not"} : kw_not)], "postprocess": id},
    {"name": "ops_between$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "ops_between", "symbols": ["ops_between$ebnf$1", "kw_between"]},
    {"name": "ops_member$subexpression$1", "symbols": [(lexerAny.has("op_member") ? {type: "op_member"} : op_member)]},
    {"name": "ops_member$subexpression$1", "symbols": [(lexerAny.has("op_membertext") ? {type: "op_membertext"} : op_membertext)]},
    {"name": "ops_member", "symbols": ["ops_member$subexpression$1"], "postprocess": x => unwrap(x)?.value},
    {"name": "expr_list_raw$ebnf$1", "symbols": []},
    {"name": "expr_list_raw$ebnf$1$subexpression$1", "symbols": ["comma", "expr_or_select"], "postprocess": last},
    {"name": "expr_list_raw$ebnf$1", "symbols": ["expr_list_raw$ebnf$1", "expr_list_raw$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "expr_list_raw", "symbols": ["expr_or_select", "expr_list_raw$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "expr_list_raw_many$ebnf$1$subexpression$1", "symbols": ["comma", "expr_or_select"], "postprocess": last},
    {"name": "expr_list_raw_many$ebnf$1", "symbols": ["expr_list_raw_many$ebnf$1$subexpression$1"]},
    {"name": "expr_list_raw_many$ebnf$1$subexpression$2", "symbols": ["comma", "expr_or_select"], "postprocess": last},
    {"name": "expr_list_raw_many$ebnf$1", "symbols": ["expr_list_raw_many$ebnf$1", "expr_list_raw_many$ebnf$1$subexpression$2"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "expr_list_raw_many", "symbols": ["expr_or_select", "expr_list_raw_many$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [unwrap(head), ...(tail || []).map(unwrap)];
        } },
    {"name": "expr_or_select$subexpression$1", "symbols": ["expr"]},
    {"name": "expr_or_select$subexpression$1", "symbols": ["select_statement"]},
    {"name": "expr_or_select", "symbols": ["expr_or_select$subexpression$1"], "postprocess": unwrap},
    {"name": "expr_list_many", "symbols": ["expr_list_raw_many"], "postprocess":  x => ({
            type: 'list',
            expressions: x[0],
        }) },
    {"name": "expr_case$ebnf$1", "symbols": ["expr"], "postprocess": id},
    {"name": "expr_case$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "expr_case$ebnf$2", "symbols": []},
    {"name": "expr_case$ebnf$2", "symbols": ["expr_case$ebnf$2", "expr_case_whens"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "expr_case$ebnf$3", "symbols": ["expr_case_else"], "postprocess": id},
    {"name": "expr_case$ebnf$3", "symbols": [], "postprocess": () => null},
    {"name": "expr_case", "symbols": [(lexerAny.has("kw_case") ? {type: "kw_case"} : kw_case), "expr_case$ebnf$1", "expr_case$ebnf$2", "expr_case$ebnf$3", (lexerAny.has("kw_end") ? {type: "kw_end"} : kw_end)], "postprocess":  x => ({
            type: 'case',
            value: x[1],
            whens: x[2],
            else: x[3],
        }) },
    {"name": "expr_case_whens", "symbols": [(lexerAny.has("kw_when") ? {type: "kw_when"} : kw_when), "expr", (lexerAny.has("kw_then") ? {type: "kw_then"} : kw_then), "expr"], "postprocess":  x => ({
            when: x[1],
            value: x[3],
        }) },
    {"name": "expr_case_else", "symbols": [(lexerAny.has("kw_else") ? {type: "kw_else"} : kw_else), "expr"], "postprocess": last},
    {"name": "expr_fn_name", "symbols": ["word"]},
    {"name": "expr_fn_name", "symbols": [(lexerAny.has("kw_any") ? {type: "kw_any"} : kw_any)]},
    {"name": "createtable_statement$ebnf$1", "symbols": ["kw_ifnotexists"], "postprocess": id},
    {"name": "createtable_statement$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "createtable_statement", "symbols": [(lexerAny.has("kw_create") ? {type: "kw_create"} : kw_create), (lexerAny.has("kw_table") ? {type: "kw_table"} : kw_table), "createtable_statement$ebnf$1", "word", "lparen", "createtable_declarationlist", "rparen"], "postprocess":  x => {
        
            const cols = x[5].filter((v: any) => 'dataType' in v);
            const constraints = x[5].filter((v: any) => !('dataType' in v));
        
            return {
                type: 'create table',
                ... !!x[2] ? { ifNotExists: true } : {},
                name: x[3],
                columns: cols,
                ...constraints.length ? { constraints } : {},
            }
        } },
    {"name": "createtable_declarationlist$ebnf$1", "symbols": []},
    {"name": "createtable_declarationlist$ebnf$1$subexpression$1", "symbols": ["comma", "createtable_declaration"], "postprocess": last},
    {"name": "createtable_declarationlist$ebnf$1", "symbols": ["createtable_declarationlist$ebnf$1", "createtable_declarationlist$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "createtable_declarationlist", "symbols": ["createtable_declaration", "createtable_declarationlist$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "createtable_declaration$subexpression$1", "symbols": ["createtable_constraint"]},
    {"name": "createtable_declaration$subexpression$1", "symbols": ["createtable_column"]},
    {"name": "createtable_declaration", "symbols": ["createtable_declaration$subexpression$1"], "postprocess": unwrap},
    {"name": "createtable_constraint$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_constraint") ? {type: "kw_constraint"} : kw_constraint), "word"], "postprocess": last},
    {"name": "createtable_constraint$ebnf$1", "symbols": ["createtable_constraint$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "createtable_constraint$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "createtable_constraint", "symbols": ["createtable_constraint$ebnf$1", "createtable_constraint_def"], "postprocess":  x => {
            const name = unwrap(x[0]);
            if (!name) {
                return unwrap(x[1]);
            }
            return {
                constraintName: name,
                ...unwrap(x[1]),
            }
        } },
    {"name": "createtable_constraint_def", "symbols": ["kw_not_null"], "postprocess": ()=> ({ type: 'not null' })},
    {"name": "createtable_constraint_def$subexpression$1", "symbols": [(lexerAny.has("kw_unique") ? {type: "kw_unique"} : kw_unique)]},
    {"name": "createtable_constraint_def$subexpression$1", "symbols": ["kw_primary_key"]},
    {"name": "createtable_constraint_def", "symbols": ["createtable_constraint_def$subexpression$1", "lparen", "createtable_collist", "rparen"], "postprocess":  x => ({
            type: flattenStr(x[0]).join(' ').toLowerCase(),
            columns: x[2],
        }) },
    {"name": "createtable_collist$ebnf$1", "symbols": []},
    {"name": "createtable_collist$ebnf$1$subexpression$1", "symbols": ["comma", "ident"], "postprocess": last},
    {"name": "createtable_collist$ebnf$1", "symbols": ["createtable_collist$ebnf$1", "createtable_collist$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "createtable_collist", "symbols": ["ident", "createtable_collist$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "createtable_column$ebnf$1", "symbols": ["createtable_column_constraint"], "postprocess": id},
    {"name": "createtable_column$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "createtable_column$ebnf$2$subexpression$1", "symbols": [(lexerAny.has("kw_default") ? {type: "kw_default"} : kw_default), "expr"], "postprocess": last},
    {"name": "createtable_column$ebnf$2", "symbols": ["createtable_column$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "createtable_column$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "createtable_column", "symbols": ["word", "data_type", "createtable_column$ebnf$1", "createtable_column$ebnf$2"], "postprocess":  x => ({
            name: x[0],
            dataType: x[1],
            ...x[2] ? { constraint: x[2] }: {},
            ...x[3] ? { default: unwrap(x[3]) } : {}
        }) },
    {"name": "createtable_column_constraint$ebnf$1", "symbols": ["kw_not_null"], "postprocess": id},
    {"name": "createtable_column_constraint$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "createtable_column_constraint", "symbols": [(lexerAny.has("kw_unique") ? {type: "kw_unique"} : kw_unique), "createtable_column_constraint$ebnf$1"], "postprocess": ([_, nn]) => ({ type: 'unique', ...!!nn ? {notNull: !!nn} : {}})},
    {"name": "createtable_column_constraint", "symbols": ["kw_primary_key"], "postprocess": () => ({ type: 'primary key' })},
    {"name": "createtable_column_constraint", "symbols": ["kw_not_null"], "postprocess": () => ({ type: 'not null' })},
    {"name": "createindex_statement$ebnf$1", "symbols": [(lexerAny.has("kw_unique") ? {type: "kw_unique"} : kw_unique)], "postprocess": id},
    {"name": "createindex_statement$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "createindex_statement$ebnf$2", "symbols": ["kw_ifnotexists"], "postprocess": id},
    {"name": "createindex_statement$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "createindex_statement$ebnf$3", "symbols": ["word"], "postprocess": id},
    {"name": "createindex_statement$ebnf$3", "symbols": [], "postprocess": () => null},
    {"name": "createindex_statement", "symbols": [(lexerAny.has("kw_create") ? {type: "kw_create"} : kw_create), "createindex_statement$ebnf$1", "kw_index", "createindex_statement$ebnf$2", "createindex_statement$ebnf$3", (lexerAny.has("kw_on") ? {type: "kw_on"} : kw_on), "word", "lparen", "createindex_expressions", "rparen"], "postprocess":  x => ({
            type: 'create index',
            ... !!x[1] ? { unique: true } : {},
            ... !!x[3] ? { ifNotExists: true } : {},
            ... !!x[4] ? { indexName: x[4] } : {},
            table: x[6],
            expressions: x[8],
        }) },
    {"name": "createindex_expressions$ebnf$1", "symbols": []},
    {"name": "createindex_expressions$ebnf$1$subexpression$1", "symbols": ["comma", "createindex_expression"], "postprocess": last},
    {"name": "createindex_expressions$ebnf$1", "symbols": ["createindex_expressions$ebnf$1", "createindex_expressions$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "createindex_expressions", "symbols": ["createindex_expression", "createindex_expressions$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "createindex_expression$subexpression$1", "symbols": ["expr_basic"]},
    {"name": "createindex_expression$subexpression$1", "symbols": ["expr_paren"]},
    {"name": "createindex_expression$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_asc") ? {type: "kw_asc"} : kw_asc)]},
    {"name": "createindex_expression$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_desc") ? {type: "kw_desc"} : kw_desc)]},
    {"name": "createindex_expression$ebnf$1", "symbols": ["createindex_expression$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "createindex_expression$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "createindex_expression$ebnf$2$subexpression$1$subexpression$1", "symbols": ["kw_first"]},
    {"name": "createindex_expression$ebnf$2$subexpression$1$subexpression$1", "symbols": ["kw_last"]},
    {"name": "createindex_expression$ebnf$2$subexpression$1", "symbols": ["kw_nulls", "createindex_expression$ebnf$2$subexpression$1$subexpression$1"], "postprocess": last},
    {"name": "createindex_expression$ebnf$2", "symbols": ["createindex_expression$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "createindex_expression$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "createindex_expression", "symbols": ["createindex_expression$subexpression$1", "createindex_expression$ebnf$1", "createindex_expression$ebnf$2"], "postprocess":  x => ({
            expression: unwrap(x[0]),
            ... !!x[1] ? { order: unwrap(x[1]).value.toLowerCase() } : {},
            ... !!x[2] ? { nulls: unwrap(x[2]) } : {},
        }) },
    {"name": "simplestatements_all", "symbols": ["simplestatements_start_transaction"]},
    {"name": "simplestatements_all", "symbols": ["simplestatements_commit"]},
    {"name": "simplestatements_all", "symbols": ["simplestatements_rollback"]},
    {"name": "simplestatements_start_transaction$subexpression$1", "symbols": ["kw_start", "kw_transaction"]},
    {"name": "simplestatements_start_transaction$subexpression$1", "symbols": ["kw_begin"]},
    {"name": "simplestatements_start_transaction", "symbols": ["simplestatements_start_transaction$subexpression$1"], "postprocess": () => ({ type: 'start transaction' })},
    {"name": "simplestatements_commit", "symbols": ["kw_commit"], "postprocess": () => ({ type: 'commit' })},
    {"name": "simplestatements_rollback", "symbols": ["kw_rollback"], "postprocess": () => ({ type: 'rollback' })},
    {"name": "insert_statement$subexpression$1", "symbols": ["kw_insert", (lexerAny.has("kw_into") ? {type: "kw_into"} : kw_into)]},
    {"name": "insert_statement$ebnf$1", "symbols": ["collist_paren"], "postprocess": id},
    {"name": "insert_statement$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "insert_statement$ebnf$2$subexpression$1", "symbols": ["kw_values", "insert_values"], "postprocess": last},
    {"name": "insert_statement$ebnf$2", "symbols": ["insert_statement$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "insert_statement$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "insert_statement$ebnf$3$subexpression$1", "symbols": ["select_statement"]},
    {"name": "insert_statement$ebnf$3$subexpression$1", "symbols": ["select_statement_paren"]},
    {"name": "insert_statement$ebnf$3", "symbols": ["insert_statement$ebnf$3$subexpression$1"], "postprocess": id},
    {"name": "insert_statement$ebnf$3", "symbols": [], "postprocess": () => null},
    {"name": "insert_statement$ebnf$4$subexpression$1", "symbols": [(lexerAny.has("kw_on") ? {type: "kw_on"} : kw_on), "kw_conflict", "insert_on_conflict"], "postprocess": last},
    {"name": "insert_statement$ebnf$4", "symbols": ["insert_statement$ebnf$4$subexpression$1"], "postprocess": id},
    {"name": "insert_statement$ebnf$4", "symbols": [], "postprocess": () => null},
    {"name": "insert_statement$ebnf$5$subexpression$1", "symbols": [(lexerAny.has("kw_returning") ? {type: "kw_returning"} : kw_returning), "select_expr_list_aliased"], "postprocess": last},
    {"name": "insert_statement$ebnf$5", "symbols": ["insert_statement$ebnf$5$subexpression$1"], "postprocess": id},
    {"name": "insert_statement$ebnf$5", "symbols": [], "postprocess": () => null},
    {"name": "insert_statement", "symbols": ["insert_statement$subexpression$1", "table_ref_aliased", "insert_statement$ebnf$1", "insert_statement$ebnf$2", "insert_statement$ebnf$3", "insert_statement$ebnf$4", "insert_statement$ebnf$5"], "postprocess":  x => {
            const columns = x[2];
            const values = x[3];
            const select = unwrap(x[4]);
            const onConflict = x[5];
            const returning = x[6];
            return {
                type: 'insert',
                into: unwrap(x[1]),
                ...columns ? { columns } : {},
                ...values ? { values } : {},
                ...select ? { select } : {},
                ...returning ? { returning } : {},
                ...onConflict ? { onConflict } : {},
            }
        } },
    {"name": "insert_values$ebnf$1", "symbols": []},
    {"name": "insert_values$ebnf$1$subexpression$1", "symbols": ["comma", "insert_value"], "postprocess": last},
    {"name": "insert_values$ebnf$1", "symbols": ["insert_values$ebnf$1", "insert_values$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "insert_values", "symbols": ["insert_value", "insert_values$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "insert_value", "symbols": ["lparen", "insert_expr_list_raw", "rparen"], "postprocess": get(1)},
    {"name": "insert_single_value$subexpression$1", "symbols": ["expr_or_select"]},
    {"name": "insert_single_value$subexpression$1", "symbols": [(lexerAny.has("kw_default") ? {type: "kw_default"} : kw_default)], "postprocess": () => 'default'},
    {"name": "insert_single_value", "symbols": ["insert_single_value$subexpression$1"], "postprocess": unwrap},
    {"name": "insert_expr_list_raw$ebnf$1", "symbols": []},
    {"name": "insert_expr_list_raw$ebnf$1$subexpression$1", "symbols": ["comma", "insert_single_value"], "postprocess": last},
    {"name": "insert_expr_list_raw$ebnf$1", "symbols": ["insert_expr_list_raw$ebnf$1", "insert_expr_list_raw$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "insert_expr_list_raw", "symbols": ["insert_single_value", "insert_expr_list_raw$ebnf$1"], "postprocess":  ([head, tail]) => {
            return [head, ...(tail || [])];
        } },
    {"name": "insert_on_conflict$ebnf$1", "symbols": ["insert_on_conflict_what"], "postprocess": id},
    {"name": "insert_on_conflict$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "insert_on_conflict", "symbols": ["insert_on_conflict$ebnf$1", "insert_on_conflict_do"], "postprocess":  ([onWhat, doWhat]) => ({
            ...onWhat ? { on: onWhat[0] } : {},
            do: doWhat,
        }) },
    {"name": "insert_on_conflict_what$subexpression$1", "symbols": ["lparen", "expr_list_raw", "rparen"], "postprocess": get(1)},
    {"name": "insert_on_conflict_what", "symbols": ["insert_on_conflict_what$subexpression$1"]},
    {"name": "insert_on_conflict_do", "symbols": [(lexerAny.has("kw_do") ? {type: "kw_do"} : kw_do), "kw_nothing"], "postprocess": () => 'do nothing'},
    {"name": "insert_on_conflict_do", "symbols": [(lexerAny.has("kw_do") ? {type: "kw_do"} : kw_do), "kw_update", "kw_set", "update_set_list"], "postprocess": x => ({ sets: last(x) })},
    {"name": "update_statement$ebnf$1", "symbols": ["select_where"], "postprocess": id},
    {"name": "update_statement$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "update_statement$ebnf$2$subexpression$1", "symbols": [(lexerAny.has("kw_returning") ? {type: "kw_returning"} : kw_returning), "select_expr_list_aliased"], "postprocess": last},
    {"name": "update_statement$ebnf$2", "symbols": ["update_statement$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "update_statement$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "update_statement", "symbols": ["kw_update", "table_ref_aliased", "kw_set", "update_set_list", "update_statement$ebnf$1", "update_statement$ebnf$2"], "postprocess":  x => {
            const where = unwrap(x[4]);
            const returning = x[5];
            return {
                type: 'update',
                table: unwrap(x[1]),
                sets: x[3],
                ...where ? {where} : {},
                ...returning ? {returning} : {},
            }
        } },
    {"name": "update_set_list$ebnf$1", "symbols": []},
    {"name": "update_set_list$ebnf$1$subexpression$1", "symbols": ["comma", "update_set"], "postprocess": last},
    {"name": "update_set_list$ebnf$1", "symbols": ["update_set_list$ebnf$1", "update_set_list$ebnf$1$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "update_set_list", "symbols": ["update_set", "update_set_list$ebnf$1"], "postprocess":  ([head, tail]) => {
            const ret = [];
            for (const _t of [head, ...(tail || [])]) {
                const t = unwrap(_t);
                if (Array.isArray(t)) {
                    ret.push(...t);
                } else {
                    ret.push(t);
                }
            }
            return ret;
        } },
    {"name": "update_set", "symbols": ["update_set_one"]},
    {"name": "update_set", "symbols": ["update_set_multiple"]},
    {"name": "update_set_one$subexpression$1", "symbols": ["expr"]},
    {"name": "update_set_one$subexpression$1", "symbols": [(lexerAny.has("kw_default") ? {type: "kw_default"} : kw_default)], "postprocess": value},
    {"name": "update_set_one", "symbols": ["ident", (lexerAny.has("op_eq") ? {type: "op_eq"} : op_eq), "update_set_one$subexpression$1"], "postprocess":  x => ({
            column: unwrap(x[0]),
            value: unwrap(x[2]),
        }) },
    {"name": "update_set_multiple$subexpression$1", "symbols": ["lparen", "expr_list_raw", "rparen"], "postprocess": get(1)},
    {"name": "update_set_multiple", "symbols": ["collist_paren", (lexerAny.has("op_eq") ? {type: "op_eq"} : op_eq), "update_set_multiple$subexpression$1"], "postprocess":  x => {
            const cols = x[0];
            const exprs = x[2];
            if (cols.length !== exprs.length) {
                throw new Error('number of columns does not match number of values');
            }
            return cols.map((x: any, i: number) => ({
                column: unwrap(x),
                value: unwrap(exprs[i]),
            }))
        } },
    {"name": "altertable_statement$ebnf$1", "symbols": ["kw_ifexists"], "postprocess": id},
    {"name": "altertable_statement$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altertable_statement", "symbols": ["kw_alter", (lexerAny.has("kw_table") ? {type: "kw_table"} : kw_table), "altertable_statement$ebnf$1", "table_ref", "altertable_action"], "postprocess":  x => ({
            type: 'alter table',
            ... x[2] ? {ifExists: true} : {},
            table: unwrap(x[3]),
            change: unwrap(x[4]),
        }) },
    {"name": "altertable_action", "symbols": ["altertable_rename_table"]},
    {"name": "altertable_action", "symbols": ["altertable_rename_column"]},
    {"name": "altertable_action", "symbols": ["altertable_rename_constraint"]},
    {"name": "altertable_action", "symbols": ["altertable_add_column"]},
    {"name": "altertable_action", "symbols": ["altertable_drop_column"]},
    {"name": "altertable_action", "symbols": ["altertable_alter_column"]},
    {"name": "altertable_action", "symbols": ["altertable_add_constraint"]},
    {"name": "altertable_rename_table", "symbols": ["kw_rename", (lexerAny.has("kw_to") ? {type: "kw_to"} : kw_to), "word"], "postprocess":  x => ({
            type: 'rename',
            to: unwrap(last(x)),
        }) },
    {"name": "altertable_rename_column$ebnf$1", "symbols": [(lexerAny.has("kw_column") ? {type: "kw_column"} : kw_column)], "postprocess": id},
    {"name": "altertable_rename_column$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altertable_rename_column", "symbols": ["kw_rename", "altertable_rename_column$ebnf$1", "ident", (lexerAny.has("kw_to") ? {type: "kw_to"} : kw_to), "ident"], "postprocess":  x => ({
            type: 'rename column',
            column: unwrap(x[2]),
            to: unwrap(last(x)),
        }) },
    {"name": "altertable_rename_constraint", "symbols": ["kw_rename", (lexerAny.has("kw_constraint") ? {type: "kw_constraint"} : kw_constraint), "ident", (lexerAny.has("kw_to") ? {type: "kw_to"} : kw_to), "ident"], "postprocess":  x => ({
            type: 'rename constraint',
            constraint: unwrap(x[2]),
            to: unwrap(last(x)),
        }) },
    {"name": "altertable_add_column$ebnf$1", "symbols": [(lexerAny.has("kw_column") ? {type: "kw_column"} : kw_column)], "postprocess": id},
    {"name": "altertable_add_column$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altertable_add_column$ebnf$2", "symbols": ["kw_ifnotexists"], "postprocess": id},
    {"name": "altertable_add_column$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "altertable_add_column", "symbols": ["kw_add", "altertable_add_column$ebnf$1", "altertable_add_column$ebnf$2", "createtable_column"], "postprocess":  x => ({
            type: 'add column',
            ... x[2] ? {ifNotExists: true} : {},
            column: unwrap(x[3]),
        }) },
    {"name": "altertable_drop_column$ebnf$1", "symbols": [(lexerAny.has("kw_column") ? {type: "kw_column"} : kw_column)], "postprocess": id},
    {"name": "altertable_drop_column$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altertable_drop_column$ebnf$2", "symbols": ["kw_ifexists"], "postprocess": id},
    {"name": "altertable_drop_column$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "altertable_drop_column", "symbols": ["kw_drop", "altertable_drop_column$ebnf$1", "altertable_drop_column$ebnf$2", "ident"], "postprocess":  x => ({
            type: 'drop column',
            ... x[2] ? {ifExists: true} : {},
            column: unwrap(x[3]),
        }) },
    {"name": "altertable_alter_column$ebnf$1", "symbols": [(lexerAny.has("kw_column") ? {type: "kw_column"} : kw_column)], "postprocess": id},
    {"name": "altertable_alter_column$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altertable_alter_column", "symbols": ["kw_alter", "altertable_alter_column$ebnf$1", "ident", "altercol"], "postprocess":  x => ({
            type: 'alter column',
            column: unwrap(x[2]),
            alter: unwrap(x[3])
        }) },
    {"name": "altercol$ebnf$1$subexpression$1", "symbols": ["kw_set", "kw_data"]},
    {"name": "altercol$ebnf$1", "symbols": ["altercol$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "altercol$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altercol", "symbols": ["altercol$ebnf$1", "kw_type", "data_type"], "postprocess": x => ({ type: 'set type', dataType: unwrap(last(x)) })},
    {"name": "altercol", "symbols": ["kw_set", (lexerAny.has("kw_default") ? {type: "kw_default"} : kw_default), "expr"], "postprocess": x => ({type: 'set default', default: unwrap(last(x)) })},
    {"name": "altercol", "symbols": ["kw_drop", (lexerAny.has("kw_default") ? {type: "kw_default"} : kw_default)], "postprocess": x => ({type: 'drop default' })},
    {"name": "altercol$subexpression$1", "symbols": ["kw_set"]},
    {"name": "altercol$subexpression$1", "symbols": ["kw_drop"]},
    {"name": "altercol", "symbols": ["altercol$subexpression$1", "kw_not_null"], "postprocess": x => ({type: flattenStr(x).join(' ').toLowerCase() })},
    {"name": "altertable_add_constraint$ebnf$1", "symbols": ["ident"], "postprocess": id},
    {"name": "altertable_add_constraint$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altertable_add_constraint", "symbols": ["kw_add", (lexerAny.has("kw_constraint") ? {type: "kw_constraint"} : kw_constraint), "altertable_add_constraint$ebnf$1", "altertable_add_constraint_kind"], "postprocess":  x => ({
            type: 'add constraint',
            ...x[2] ? { constraintName: unwrap(x[2]) } : {},
            constraint: unwrap(last(x)),
        }) },
    {"name": "altertable_add_constraint_kind", "symbols": ["altertable_add_constraint_foreignkey"]},
    {"name": "altertable_add_constraint_kind", "symbols": ["altertable_add_constraint_primarykey"]},
    {"name": "altertable_add_constraint_primarykey", "symbols": [(lexerAny.has("kw_primary") ? {type: "kw_primary"} : kw_primary), "kw_key", "collist_paren"], "postprocess":  x => ({
            type: 'primary key',
            columns: x[2],
        }) },
    {"name": "altertable_add_constraint_foreignkey$ebnf$1$subexpression$1", "symbols": [(lexerAny.has("kw_on") ? {type: "kw_on"} : kw_on), "kw_delete", "altertable_on_action"], "postprocess": last},
    {"name": "altertable_add_constraint_foreignkey$ebnf$1", "symbols": ["altertable_add_constraint_foreignkey$ebnf$1$subexpression$1"], "postprocess": id},
    {"name": "altertable_add_constraint_foreignkey$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "altertable_add_constraint_foreignkey$ebnf$2$subexpression$1", "symbols": [(lexerAny.has("kw_on") ? {type: "kw_on"} : kw_on), "kw_update", "altertable_on_action"], "postprocess": last},
    {"name": "altertable_add_constraint_foreignkey$ebnf$2", "symbols": ["altertable_add_constraint_foreignkey$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "altertable_add_constraint_foreignkey$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "altertable_add_constraint_foreignkey", "symbols": [(lexerAny.has("kw_foreign") ? {type: "kw_foreign"} : kw_foreign), "kw_key", "collist_paren", (lexerAny.has("kw_references") ? {type: "kw_references"} : kw_references), "ident", "collist_paren", "altertable_add_constraint_foreignkey$ebnf$1", "altertable_add_constraint_foreignkey$ebnf$2"], "postprocess":  x => ({
            type: 'foreign key',
            localColumns: x[2],
            foreignTable: unwrap(x[4]),
            foreignColumns: x[5],
            onDelete: x[6] || 'no action',
            onUpdate: x[7] || 'no action',
        }) },
    {"name": "altertable_on_action$subexpression$1", "symbols": ["kw_cascade"]},
    {"name": "altertable_on_action$subexpression$1$subexpression$1", "symbols": ["kw_no", "kw_action"]},
    {"name": "altertable_on_action$subexpression$1", "symbols": ["altertable_on_action$subexpression$1$subexpression$1"]},
    {"name": "altertable_on_action$subexpression$1", "symbols": ["kw_restrict"]},
    {"name": "altertable_on_action$subexpression$1$subexpression$2", "symbols": [(lexerAny.has("kw_null") ? {type: "kw_null"} : kw_null)]},
    {"name": "altertable_on_action$subexpression$1$subexpression$2", "symbols": [(lexerAny.has("kw_default") ? {type: "kw_default"} : kw_default)]},
    {"name": "altertable_on_action$subexpression$1", "symbols": ["kw_set", "altertable_on_action$subexpression$1$subexpression$2"]},
    {"name": "altertable_on_action", "symbols": ["altertable_on_action$subexpression$1"], "postprocess": x => flattenStr(x).join(' ').toLowerCase()},
    {"name": "delete_statement", "symbols": ["delete_delete"]},
    {"name": "delete_statement", "symbols": ["delete_truncate"]},
    {"name": "delete_delete$subexpression$1", "symbols": ["kw_delete", (lexerAny.has("kw_from") ? {type: "kw_from"} : kw_from)]},
    {"name": "delete_delete$ebnf$1", "symbols": ["select_where"], "postprocess": id},
    {"name": "delete_delete$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "delete_delete$ebnf$2$subexpression$1", "symbols": [(lexerAny.has("kw_returning") ? {type: "kw_returning"} : kw_returning), "select_expr_list_aliased"], "postprocess": last},
    {"name": "delete_delete$ebnf$2", "symbols": ["delete_delete$ebnf$2$subexpression$1"], "postprocess": id},
    {"name": "delete_delete$ebnf$2", "symbols": [], "postprocess": () => null},
    {"name": "delete_delete", "symbols": ["delete_delete$subexpression$1", "table_ref_aliased", "delete_delete$ebnf$1", "delete_delete$ebnf$2"], "postprocess":  x => {
            const where = x[2];
            const returning = x[3];
            return {
                type: 'delete',
                from: unwrap(x[1]),
                ...where ? { where } : {},
                ...returning ? { returning } : {},
            }
        } },
    {"name": "delete_truncate$subexpression$1$ebnf$1", "symbols": [(lexerAny.has("kw_table") ? {type: "kw_table"} : kw_table)], "postprocess": id},
    {"name": "delete_truncate$subexpression$1$ebnf$1", "symbols": [], "postprocess": () => null},
    {"name": "delete_truncate$subexpression$1", "symbols": ["kw_truncate", "delete_truncate$subexpression$1$ebnf$1"]},
    {"name": "delete_truncate", "symbols": ["delete_truncate$subexpression$1", "table_ref_aliased"], "postprocess":  x => ({
            type: 'delete',
            from: unwrap(x[1]),
        }) },
    {"name": "main$ebnf$1", "symbols": []},
    {"name": "main$ebnf$1", "symbols": ["main$ebnf$1", "statement_separator"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "main$ebnf$2", "symbols": []},
    {"name": "main$ebnf$2$subexpression$1$ebnf$1", "symbols": ["statement_separator"]},
    {"name": "main$ebnf$2$subexpression$1$ebnf$1", "symbols": ["main$ebnf$2$subexpression$1$ebnf$1", "statement_separator"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "main$ebnf$2$subexpression$1", "symbols": ["main$ebnf$2$subexpression$1$ebnf$1", "statement"]},
    {"name": "main$ebnf$2", "symbols": ["main$ebnf$2", "main$ebnf$2$subexpression$1"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "main$ebnf$3", "symbols": []},
    {"name": "main$ebnf$3", "symbols": ["main$ebnf$3", "statement_separator"], "postprocess": (d) => d[0].concat([d[1]])},
    {"name": "main", "symbols": ["main$ebnf$1", "statement", "main$ebnf$2", "main$ebnf$3"], "postprocess":  ([_, head, _tail]) => {
            const tail = _tail; // && _tail[0];
            const first = unwrap(head);
            first[LOCATION] = { start: 0 };
            if (!tail || !tail.length) {
                return first;
            }
            const ret = [first];
            let prev = first;
            for (const t of tail) {
                const firstSep = unwrap(t[0][0]);
                prev[LOCATION].end = firstSep.offset;
        
                const lastSep = unwrap(last(t[0]));
                const statement = unwrap(t[1]);
                statement[LOCATION] = {
                    start: lastSep.offset,
                };
                prev = statement;
                ret.push(statement);
            }
            return ret;
        } },
    {"name": "statement_separator", "symbols": [(lexerAny.has("semicolon") ? {type: "semicolon"} : semicolon)]},
    {"name": "statement", "symbols": ["select_statement"]},
    {"name": "statement", "symbols": ["createtable_statement"]},
    {"name": "statement", "symbols": ["createindex_statement"]},
    {"name": "statement", "symbols": ["simplestatements_all"]},
    {"name": "statement", "symbols": ["insert_statement"]},
    {"name": "statement", "symbols": ["update_statement"]},
    {"name": "statement", "symbols": ["altertable_statement"]},
    {"name": "statement", "symbols": ["delete_statement"]}
  ],
  ParserStart: "main",
};

export default grammar;
