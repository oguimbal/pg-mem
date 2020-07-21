import { Parser, Grammar } from 'nearley';
import { expect, assert } from 'chai';
import grammar from '../syntax/main.ne';
import { trimNullish } from '../../utils';
import { Value, SelectStatement, CreateTableStatement, CreateIndexStatement } from './ast';

export function checkSelect(value: string | string[], expected: SelectStatement)  {
    checkTree(value, expected);
}
export function checkCreateTable(value: string | string[], expected: CreateTableStatement)  {
    checkTree(value, expected);
}
export function checkCreateIndex(value: string | string[], expected: CreateIndexStatement)  {
    checkTree(value, expected);
}

function checkTree<T>(value: string | string[], expected: T, start?: string) {
    if (typeof value === 'string') {
        value = [value];
    }
    for (const sql of value) {
        it('parses ' + sql, () => {
            const gram = Grammar.fromCompiled(grammar);
            if (start) {
                gram.start = start
            }
            const parser = new Parser(gram);
            parser.feed(sql);
            const ret = parser.finish();
            if (!ret.length) {
                assert.fail('Unexpected end of input');
            }
            expect(ret.length).to.equal(1, 'Ambiguous matches')
            expect(trimNullish(ret[0]))
                .to.deep.equal(expected);
        });
    }
}

export function checkInvalid(sql: string, start?: string) {
    it('does not parses ' + sql, () => {
        const gram = Grammar.fromCompiled(grammar);
        if (start) {
            gram.start = start
        }
        assert.throws(() => {
            const parser = new Parser(gram);
            parser.feed(sql);
            expect(parser.results).not.to.deep.equal([]);
        });
    });
}

export function checkTreeExpr(value: string | string[], expected: Value) {
    checkTree(value, expected, 'expr');
}