import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { CompiledFunction, DataType, IMemoryDb } from '../interfaces';
import { expectQueryError, preventSeqScan } from './test-utils';

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
            .toEqual([{ concat: 'abc' }]);
        expect(many(`SELECT ConCat('a', 'b', 'c')`))
            .toEqual([{ concat: 'abc' }]);
        expectQueryError(() => many(`SELECT "ConCat"('a', 'b', 'c')`), /does not exist/);
    });

    it('accepts nulls in concat', () => {
        expect(many(`select concat('text-', null, 123, null, '-end');`))
            .toEqual([{ concat: 'text-123-end' }]);
    });

    it('GREATEST 2 arguments', () => {
        expect(many(`select GREATEST(0, -1);`))
            .toEqual([{ greatest: 0 }]);
    });

    it('GREATEST 4 arguments', () => {
        expect(many(`select GREATEST(3, 8, 10, 4);`))
            .toEqual([{ greatest: 10 }]);
    });

    it('LEAST 2 arguments', () => {
        expect(many(`select LEAST(0, -1);`))
            .toEqual([{ least: -1 }]);
    });

    it('LEAST 4 arguments', () => {
        expect(many(`select LEAST(3, 8, 1, 4);`))
            .toEqual([{ least: 1 }]);
    });

    it('can declare & call function', () => {
        db.registerLanguage('mylang', ({ code, args, returns }) => {
            expect(code).toBe('some code');
            expect(args.map(x => x.type.primary)).toEqual([DataType.text]);
            expect(returns?.primary).toBe(DataType.text);
            return arg => {
                return 'hello ' + arg;
            }
        });

        none(`CREATE FUNCTION "sayHello"(in arg text) RETURNS text
        AS $$some code$$
        LANGUAGE mylang`);

        expectQueryError(() => many(`select sayHello('world')`), /does not exist/);

        expect(many(`select "sayHello"('world')`))
            .toEqual([{ sayHello: 'hello world' }]);
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
            .toEqual([{ calc_plv8: 10 }])
    })


    it('can execute do statement', () => {
        let called = false;
        db.registerLanguage('mylang', () => {
            return () => {
                called = true;
            }
        });

        none(`DO LANGUAGE mylang $$some code$$`);

        expect(called).toBeTrue();
    });

    it('does not call when has null argument', () => {
        db.public.registerFunction({
            name: 'myfn',
            args: [DataType.text],
            returns: DataType.text,
            implementation: () => {
                expect('Should not be called').toBe('');
            },
        });

        expect(many(`select myfn(null)`))
            .toEqual([{ myfn: null }]);
    });

    it('calls when has null argument and told it to', () => {
        db.public.registerFunction({
            name: 'myfn',
            args: [DataType.text],
            returns: DataType.text,
            allowNullArguments: true,
            implementation: v => {
                expect(v).toBe(null);
                return 'hi !';
            },
        });

        expect(many(`select myfn(null)`))
            .toEqual([{ myfn: 'hi !' }]);
    });

    it('[bugfix] supports coalesce() with implicit cast arguments', () => {
        // this was throwing ("cannot cast type timestamp to text"):
        const val1 = one(`select COALESCE('2021-12-07T13:49:53.458Z', '2021-12-07T13:49:53.458Z'::timestamp) x`).x;
        expect(val1).toBeInstanceOf(Date);
        // this one was not
        const val2 = one(`select COALESCE('2021-12-07T13:49:53.458Z'::timestamp, '2021-12-07T13:49:53.458Z') x`).x;
        expect(val2).toBeInstanceOf(Date);

        expectQueryError(() => none(`select COALESCE(42, '2021-12-07T13:49:53.458Z'::timestamp)`), /COALESCE types integer and timestamp without time zone cannot be matched/);
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
            .toEqual([{ row_to_json: { a: 1, b: 2 } }]);
    });


    it('resolves arg cast from constant literal', () => {
        db.public.registerFunction({
            name: 'jsonb_array_length',
            args: [DataType.jsonb],
            returns: DataType.integer,
            implementation: a => a.length,
        });

        expect(many(`select jsonb_array_length('[42,51]') result`))
            .toEqual([{ result: 2 }])
        expectQueryError(() => many(`select jsonb_array_length('test') result`), /invalid input syntax/);
    });

    it('resolves variadic cast from constant literal', () => {
        db.public.registerFunction({
            name: 'jsonb_array_length',
            argsVariadic: DataType.jsonb,
            returns: DataType.integer,
            implementation: a => a.length,
        });

        expect(many(`select jsonb_array_length('[42,51]') result`))
            .toEqual([{ result: 2 }])
        expectQueryError(() => many(`select jsonb_array_length('test') result`), /invalid input syntax/);
    });
});
