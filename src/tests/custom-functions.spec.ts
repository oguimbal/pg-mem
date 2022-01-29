import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';

describe('Custom functions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('can declare a custom implementation of plpgsql language', () => {
        let executed = false;
        db.registerLanguage('plpgsql', ({ code, args, returns }) => {
            expect(code).to.equal('whatever');
            return () => {
                executed = true;
            };
        });
        none(` do $$whatever$$;`);
        assert.isTrue(executed, 'Cusotm language was not executed');
    });

    it('can declare a custom language', () => {
        db.registerLanguage('mylanguage', ({ code, args, returns }) => {
            expect(code).to.equal('whatever');
            return arg => {
                expect(arg).to.equal('some arg');
                return 42;
            };
        });

        none(`CREATE FUNCTION test_fn(arg text) RETURNS int
                AS $$whatever$$
                LANGUAGE mylanguage;`)

        expect(one(`SELECT test_fn('some arg');`)).to.deep.equal({ test_fn: 42 });
    });

    it('can use sql language as single value', () => {
        none(`CREATE FUNCTION test_fn(arg text) RETURNS int
                AS $$ select 42 a $$
                LANGUAGE SQL;`)


        expect(one(`SELECT test_fn('some arg');`)).to.deep.equal({ test_fn: 42 });
    });


    it('can use sql language arguments in single value', () => {
        none(`CREATE FUNCTION test_fn(hello text) RETURNS text
                AS $$ select hello || ' world' $$
                LANGUAGE SQL;`)


        expect(one(`SELECT test_fn('hello');`)).to.deep.equal({ test_fn: 'hello world' });
    });

    it('can use sql language positional arguments in single value', () => {
        none(`CREATE FUNCTION test_fn(hello text) RETURNS text
                AS $$ select $1 || ' world' $$
                LANGUAGE SQL;`)

        expect(one(`SELECT test_fn('hello');`)).to.deep.equal({ test_fn: 'hello world' });
    });

    it('can override sql language', () => {
        db.registerLanguage('sql', ({ code, args, returns }) => {
            expect(code).to.equal('whatever');
            return arg => {
                expect(arg).to.equal('some arg');
                return 42;
            };
        });

        none(`CREATE FUNCTION test_fn(arg text) RETURNS int
                AS $$whatever$$
                LANGUAGE SQL;`)

        expect(one(`SELECT test_fn('some arg');`)).to.deep.equal({ test_fn: 42 });
    });


    it('can use sql language as table with * subselection', () => {
        expect(many(`CREATE FUNCTION test_fn() RETURNS  table (val text) stable
                AS $$ select * from (values('a') ) as foo(val) $$
                LANGUAGE SQL;

                SELECT * from test_fn();`)).to.deep.equal([{ val: 'a' }]);
    });

    it('can use sql language as table with column subselection', () => {
        expect(many(`CREATE FUNCTION test_fn() RETURNS  table (a text, b text) stable
                AS $$ select * from (values('sarah ', 'conor') ) as foo(x, y) $$
                LANGUAGE SQL;

                SELECT 'hi ' || a || b as col from test_fn();`))
            .to.deep.equal([{ col: 'hi sarah conor' }]);
    });

    it('can reuse table function with different arguments in same statement', () => {
        expect(many(`CREATE FUNCTION test_fn(arg text) RETURNS  table (sentence text) stable
                AS $$ select * from (values('hello ' || arg) ) as foo(x) $$
                LANGUAGE SQL;

                select * from test_fn('world') union select * from test_fn('sarah conor');`))
            .to.deep.equal([{ sentence: 'hello world' }, { sentence: 'hello sarah conor' }]);
    });

    it('can reuse value function with different arguments in same statement', () => {
        expect(many(`CREATE FUNCTION test_fn(arg text) RETURNS  text
                AS $$ select 'hello ' || arg $$
                LANGUAGE SQL;

                select test_fn('world') || ' and ' || test_fn('sarah conor') as col;`))
            .to.deep.equal([{ col: 'hello world and hello sarah conor' }]);
    })

    it('can use sql language as table in direct select', () => {
        expect(many(`CREATE FUNCTION test_fn() RETURNS  table (val text) stable
                AS $$ select * from (values('a') ) as foo(val) $$
                LANGUAGE SQL;

                SELECT test_fn();`))
            .to.deep.equal([{ val: 'a' }]);
    });


    it('can use arguments in table function', () => {
        expect(many(`CREATE FUNCTION test_fn(arg text) RETURNS  table (val text) stable
                AS $$ select * from (values('hello ' || $1) , ('bye ' || $1) ) as foo(val) $$
                LANGUAGE SQL;

                SELECT * from test_fn('world') where val like 'hello%';`))

            .to.deep.equal([{ val: 'hello world' }]);
    });



    it('does not take parameters into account when overriden', () => {
        expect(one(`
        CREATE FUNCTION test_fn(hello text) RETURNS text
                AS $$ select hello from (values ('inner')) as tbl(hello) $$
                LANGUAGE SQL;


               select test_fn('hello');
        `)).to.deep.equal({ test_fn: 'inner' });
    })

    describe('should check things', () => {

        it('cannot change type of function', () => {
            none(`CREATE FUNCTION test_fn() RETURNS TEXT
                    AS $$ select 'hello' $$
                    LANGUAGE SQL;`);

            assert.throws(() => none(`CREATE OR REPLACE FUNCTION test_fn() RETURNS INT
                AS $$ select 42 $$
                LANGUAGE SQL;`), /cannot change return type of existing function/);
        });

        it('cannot change args of function', () => {
            none(`CREATE FUNCTION test_fn(arg text) RETURNS TEXT
                    AS $$ select 'hello' $$
                    LANGUAGE SQL;`);

            assert.throws(() => none(`CREATE OR REPLACE FUNCTION test_fn(new_arg text) RETURNS TEXT
                AS $$ select 'hello' $$
                LANGUAGE SQL;`), /cannot change name of input parameter "arg"/);
        });

        it('checks return type on single values', () => {
            assert.throws(() => none(`CREATE or replace FUNCTION test_fn() RETURNS INT
            AS $$ select 'hello' $$
            LANGUAGE SQL`), /return type mismatch in function declared to return integer/); // error 42P13
        });

        it('checks column count on single values', () => {
            assert.throws(() => none(`CREATE FUNCTION test_fn() RETURNS INT
            AS $$ select * from (values(42, 'a') ) as foo(a,b) $$
            LANGUAGE SQL`), /return type mismatch in function declared to return integer/); // error 42P13
        });

        it('checks column count on table values', () => {
            assert.throws(() => none(`CREATE FUNCTION test_fn() RETURNS table (a int, b int) stable
            AS $$ select * from (values('a') ) as foo(val) $$
            LANGUAGE SQL`), /return type mismatch in function declared to return record/); // error 42P13
        });

        it('checks column types on table values', () => {
            assert.throws(() => none(`CREATE FUNCTION test_fn() RETURNS table (a int, b int) stable
            AS $$ select * from (values(42, 'a') ) as foo(a,b) $$
            LANGUAGE SQL`), /return type mismatch in function declared to return record/); // error 42P13
        });
    })
});
