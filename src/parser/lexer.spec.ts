import 'mocha';
import 'chai';
import { lexer } from './lexer';
import { expect, assert } from 'chai';

describe('[PG syntax] Lexer', () => {

    const hasContent = [
        /^word$/,
        /^int$/,
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

    it('tokenizes end comment', () => {
        lexer.reset(`SELECT -- test`);
        next({ type: 'kw_select' });
    });

    it('tokenizes middle comment', () => {
        lexer.reset(`SELECT -- test\n*`);
        next({ type: 'kw_select' });
        next({ type: 'star' });
    });


    it('tokenizes middle comment after op', () => {
        lexer.reset(`2+-- yo\n2`);
        next({ type: 'int', value: '2' });
        next({ type: 'op_plus' });
        next({ type: 'int', value: '2' });
    });

    it('tokenizes star comment', () => {
        lexer.reset(`SELECT /* test */ *`);
        next({ type: 'kw_select' });
        next({ type: 'star' });
    });

    it('tokenizes select', () => {
        lexer.reset(`SELECT * FROM test`);
        next({ type: 'kw_select' });
        next({ type: 'star' });
        next({ type: 'kw_from' });
        next({ type: 'word', value: 'test' });
    });

    it('tokenizes select without spaces', () => {
        lexer.reset(`SELECT(id)from"test"`);
        next({ type: 'kw_select' });
        next({ type: 'lparen' });
        next({ type: 'word', value: 'id' });
        next({ type: 'rparen' });
        next({ type: 'kw_from' });
        next({ type: 'word', value: '"test"' });
    });

    it('tokenizes additive binaries', () => {
        lexer.reset('2+2');
        next({ type: 'int', value: '2' });
        next({ type: 'op_plus' });
        next({ type: 'int', value: '2' });
    });

    it('tokenizes comma', () => {
        lexer.reset('2,2');
        next({ type: 'int', value: '2' });
        next({ type: 'comma' });
        next({ type: 'int', value: '2' });
    })


    it('tokenizes ->', () => {
        lexer.reset('a->b');
        next({ type: 'word', value: 'a' });
        next({ type: 'op_member' });
        next({ type: 'word', value: 'b' });
    });

    it('tokenizes ->>', () => {
        lexer.reset('a->>b');
        next({ type: 'word', value: 'a' });
        next({ type: 'op_membertext' });
        next({ type: 'word', value: 'b' });
    });
});