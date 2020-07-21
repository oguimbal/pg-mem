import { Statement, Expr } from './syntax/ast';
import { Parser, Grammar } from 'nearley';
import grammar from './syntax/main.ne';
import { QueryError } from '../interfaces-private';

let gram: Grammar;

export function parse(sql: string): Statement | Statement[];
export function parse(sql: string, entry: 'expr'): Expr;
export function parse(sql: string, entry?: string): any {
    if (!gram) {
        gram = Grammar.fromCompiled(grammar);
    }
    gram.start = entry ?? 'main';
    const parser = new Parser(gram);
    parser.feed(sql);
    const asts = parser.finish();
    if (!asts.length) {
        throw new QueryError('Unexpected end of input');
    } else if (asts.length !== 1) {
        throw new QueryError('Ambiguous syntax: Please file an issue stating the request that has failed:\n' + sql);
    }
    return asts[0];
}