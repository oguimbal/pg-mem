import { Statement, Expr } from './syntax/ast';
import { Parser, Grammar } from 'nearley';
import sqlGrammar from './syntax/main.ne';
import arrayGrammar from './literal-syntaxes/array.ne';
import { QueryError } from '../interfaces-private';

let sqlCompiled: Grammar;
let arrayCompiled: Grammar;

export function parse(sql: string): Statement | Statement[];
export function parse(sql: string, entry: 'expr'): Expr;
export function parse(sql: string, entry?: string): any {
    if (!sqlCompiled) {
        sqlCompiled = Grammar.fromCompiled(sqlGrammar);
    }
    return _parse(sql, sqlCompiled, entry);
}

export function parseArrayLiteral(sql: string): any {
    if (!arrayCompiled) {
        arrayCompiled = Grammar.fromCompiled(arrayGrammar);
    }
    const val = _parse(sql, arrayCompiled);
    return val;
}

function _parse(sql: string, grammar: Grammar, entry?: string): any {
    grammar.start = entry ?? 'main';
    const parser = new Parser(grammar);
    parser.feed(sql);
    const asts = parser.finish();
    if (!asts.length) {
        throw new QueryError('Unexpected end of input');
    } else if (asts.length !== 1) {
        throw new QueryError('Ambiguous syntax: Please file an issue stating the request that has failed:\n' + sql);
    }
    return asts[0];
}
