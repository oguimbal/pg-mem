import 'mocha';
import 'chai';
import { lexer } from './array-lexer';
import { expect, assert } from 'chai';
import { parseArrayLiteral } from '../parser';

describe('PG syntax: Array literals', () => {

    const hasContent = [
        /^value$/,
    ]
    function next(expected: any) {
        const result = lexer.next();
        delete result.toString;
        delete result.col;
        delete result.line;
        delete result.lineBreaks;
        delete result.offset;
        delete result.text;
        if (!hasContent.some(x => x.test(result.type))) {
            delete result.value;
        }
        expect(result).to.deep.equal(expected);
    }

    it('Lexer: tokenizes simple list', () => {
        lexer.reset(`{  a b , " a b " , "a\\" b"}`);
        next({ type: 'start_list' });
        next({ type: 'value', value: 'a b' });
        next({ type: 'comma' });
        next({ type: 'value', value: ' a b ' });
        next({ type: 'comma' });
        next({ type: 'value', value: 'a" b' });
        next({ type: 'end_list' });
    });

    it ('parses single array', () => {
        expect(parseArrayLiteral('{a}')).to.deep.equal(['a'])
    })

    it ('parses double array', () => {
        expect(parseArrayLiteral('{a, b}')).to.deep.equal(['a', 'b'])
    })

    it ('parses two dimensions', () => {
        expect(parseArrayLiteral('{{a}, {b, c}}')).to.deep.equal([['a'], ['b', 'c']])
    })
});