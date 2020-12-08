import 'mocha';
import { expect, assert } from 'chai';
import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { _ITable } from 'interfaces-private';

// todo
describe.skip('regtype', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('can select native type as regtype', () => {
        expect(many(`select 'text'::regtype`))
            .to.deep.equal([{ regtype: 'text' }])
    })

    it('can select native with arg type as regtype', () => {
        expect(many(`select 'varchar(42)'::regtype as a, 'varchar(51)'::regtype as b, 'varchar(42)'::regtype::integer as c, 'varchar(51)'::regtype::integer as d`))
            .to.deep.equal([{
                a: 'character varying',
                b: 'character varying',
                c: 123, // todo compute this 123 (must be the same for both)
                d: 123,
            }])
    })

    it('can table as regtype', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regtype`))
            .to.deep.equal([{
                regtype: 'test'
            }])
    });

    it('fails on non existing type', () => {
        assert.throws(() => none(`select 'xxx'::regtype;`), /type "xxx" does not exist/);
    });


    it('can cast back to string', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regtype::text`))
            .to.deep.equal([{
                text: 'test'
            }])
    });


    it('can cast back table to integer', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regtype::integer`))
            .to.deep.equal([{
                integer: (db.public.getTable('test') as _ITable).reg.typeId
            }])
    });

    it('can cast back native to integer', () => {
        expect(many(`select 'text'::regtype::integer`))
            .to.deep.equal([{
                integer: 123, // todo
            }])
    });


    it('can cast existing from int', () => {
        none(`create table test (a text);`);
        const rt = (db.public.getTable('test') as _ITable).reg.typeId;
        assert.isNumber(rt);
        expect(many(`select ${rt}::regtype as `))
            .to.deep.equal([{
                regtype: 'test',
            }])
    });

    it('can cast non existing from int', () => {
        expect(many(`select 42424242::regtype`))
            .to.deep.equal([{
                regtype: 42424242,
            }]);;
    })
});