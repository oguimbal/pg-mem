import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { CompiledFunction, DataType, IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';

describe('Functions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('does not pay attention to casing', () => {
        expect(many(`SELECT CONCAT('a', 'b', 'c')`))
            .to.deep.equal([{ concat: 'abc' }]);
        expect(many(`SELECT ConCat('a', 'b', 'c')`))
            .to.deep.equal([{ concat: 'abc' }]);
        assert.throws(() => many(`SELECT "ConCat"('a', 'b', 'c')`), /does not exist/);
    });

    it('accepts nulls in concat', () => {
        expect(many(`select concat('text-', null, 123, null, '-end');`))
            .to.deep.equal([{ concat: 'text-123-end' }]);
    });

    it('can declare & call function', () => {
        db.registerLanguage('mylang', ({ code, args, returns }) => {
            expect(code).to.equal('some code');
            expect(args.map(x => x.type.primary)).to.deep.equal([DataType.text]);
            expect(returns?.primary).to.equal(DataType.text);
            return arg => {
                return 'hello ' + arg;
            }
        });

        none(`CREATE FUNCTION "sayHello"(in arg text) RETURNS text
        AS $$some code$$
        LANGUAGE mylang`);

        assert.throws(() => many(`select sayHello('world')`), /does not exist/);

        expect(many(`select "sayHello"('world')`))
            .to.deep.equal([{ sayHello: 'hello world' }]);
    });

    it('can compile kind-of plv8', () => {
        db.registerLanguage('plv8', ({ code, args }) => {
            const argNames = args.map((x, i) => x.name ?? ('$' + i));
            return new Function(...argNames, code) as CompiledFunction;
        });

        expect(many(`create or replace function calc_plv8(x int, y int, func text)
    returns int
    as
    $$
    if (func === '+'){
        return x + y
    }
    else if (func === '-'){
        return x - y
    }
    else if (func === '*'){
        return x * y
    }
    else if (func === '/'){
        return x - y
    } else {
        plv8.elog(ERROR, 'invaid function');
    }
    $$
    language plv8;

    select calc_plv8(5,5,'+')`))
            .to.deep.equal([{ calc_plv8: 10 }])
    })


    it('can execute do statement', () => {
        let called = false;
        db.registerLanguage('mylang', () => {
            return () => {
                called = true;
            }
        });

        none(`DO LANGUAGE mylang $$some code$$`);

        assert.isTrue(called);
    });

    it('does not call when has null argument', () => {
        db.public.registerFunction({
            name: 'myfn',
            args: [DataType.text],
            returns: DataType.text,
            implementation: () => {
                assert.fail('Should not be called');
            },
        });

        expect(many(`select myfn(null)`))
            .to.deep.equal([{ myfn: null }]);
    });

    it('calls when has null argument and told it to', () => {
        db.public.registerFunction({
            name: 'myfn',
            args: [DataType.text],
            returns: DataType.text,
            allowNullArguments: true,
            implementation: v => {
                expect(v).to.equal(null);
                return 'hi !';
            },
        });

        expect(many(`select myfn(null)`))
            .to.deep.equal([{ myfn: 'hi !' }]);
    });

    it('[bugfix] supports coalesce() with implicit cast arguments', () => {
        // this was throwing ("cannot cast type timestamp to text"):
        const val1 = one(`select COALESCE('2021-12-07T13:49:53.458Z', '2021-12-07T13:49:53.458Z'::timestamp) x`).x;
        assert.instanceOf(val1, Date);
        // this one was not
        const val2 = one(`select COALESCE('2021-12-07T13:49:53.458Z'::timestamp, '2021-12-07T13:49:53.458Z') x`).x;
        assert.instanceOf(val2, Date);

        assert.throws(() => none(`select COALESCE(42, '2021-12-07T13:49:53.458Z'::timestamp)`), /COALESCE types integer and timestamp without time zone cannot be matched/);
    });

    it('row_to_json() special function', () => {
        db.public.registerFunction({
            name: 'row_to_json',
            args: [DataType.record],
            returns: DataType.json,
            implementation: x => JSON.parse(JSON.stringify(x)),
        });

        expect(many(`create table example(a int, b int);
        insert into example values (1, 2);

        select row_to_json(e)  from example e `))
            .to.deep.equal([{ row_to_json: { a: 1, b: 2 } }]);
    })
});
