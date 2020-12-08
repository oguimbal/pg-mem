import 'mocha';
import { expect, assert } from 'chai';
import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { _ITable } from 'interfaces-private';

describe('regclass', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('can select pg_catalog tables as regclass', () => {
        expect(many(`select 'pg_class'::regclass`))
            .to.deep.equal([{ regclass: 'pg_class' }])
    })

    it('can select local table as regclass', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regclass`))
            .to.deep.equal([{
                regclass: 'test'
            }])
    });

    it ('fails on non existing type', () => {
        assert.throws(() => none(`select 'xxx'::regclass;`), /relation "xxx" does not exist/);
        assert.throws(() => none(`select 'text'::regclass;`), /relation "text" does not exist/);
    });


    it('can cast back to string', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regclass::text`))
            .to.deep.equal([{
                text: 'test'
            }])
    });


    it('can cast back table to integer', () => {
        expect(many(`create table test (a text);
                select 'TeSt'::regclass::integer`))
            .to.deep.equal([{
                integer: (db.public.getTable('test') as _ITable).reg.classId
            }])
    });


    it('can cast existing from int', () => {
        none(`create table test (a text);`);
        const rt = (db.public.getTable('test') as _ITable).reg.classId;
        assert.isNumber(rt);
        expect(many(`select ${rt}::regclass as asint, '${rt}'::regclass as asstr`))
            .to.deep.equal([{
                asint: 'test',
                asstr: 'test',
            }])
    });

    it('can cast non existing from int', () => {
        expect(many(`select 42424242::regclass`))
            .to.deep.equal([{
                regclass: 42424242,
            }]);;
    })


    it('cannot cast from invalid type name', () => {
        assert.throws(() => many(`select 25abc::regclass`), /invalid type name "25s"/)
    })
});