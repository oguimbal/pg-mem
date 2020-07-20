import { Parser, Grammar } from 'nearley';
import grammar from './postgresql.ne';

export function createParser() {
    const parser = new Parser(Grammar.fromCompiled(grammar));
    return parser;
}