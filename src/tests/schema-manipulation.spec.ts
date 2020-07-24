import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { watchUse } from '../utils';
import { preventSeqScan } from './test-utils';
import moment from 'moment';

describe('Schema manipulation', () => {

    it('table with primary', () => {
        const db = newDb();
        db.public.none(`create table test(id text primary key, value text)`);
        preventSeqScan(db, 'test');
        db.public.none(`insert into test(id, value) values ('A', 'Value A')`);
        const many = db.public.many(`select value from test where id='A'`);
        expect(many).to.deep.equal([{ value: 'Value A' }]);
    });

    it('table without primary', () => {
        const db = newDb();
        db.public.none(`create table test(value text)`);
        db.public.none(`insert into test(value) values ('Value A')`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 'Value A' }]);
    });

    it('table int', () => {
        const db = newDb();
        db.public.none(`create table test(value int)`);
        db.public.none(`insert into test(value) values (42.5)`);
        const many = db.public.many(`select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 43 }]); // must be rounded (int)
    });

    it('table integer', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value integer);
                        insert into test(value) values (42.5);
                        select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 43 }]); // must be rounded (int)
    });

    it('table varchar', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value varchar);
                        insert into test(value) values ('test');
                        select value from test where value is not null;`);
        expect(many).to.deep.equal([{ value: 'test' }]);
    });

    it('table varchar(n)', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value varchar(5));
                        insert into test(value) values ('test');
                        select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 'test' }]);
    });

    it('table varchar(n) with insert too long', () => {
        const db = newDb();
        db.public.none(`create table test(value varchar(5))`);
        assert.throws(() => {
            db.public.none(`insert into test(value) values ('12345678')`);
        });
    });


    it('table float', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value float);
                        insert into test(value) values (42.5);
                        select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });

    it('table decimal', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value decimal);;
                                    insert into test(value) values (42.5);
                                    select value from test where value is not null`);
        expect(many).to.deep.equal([{ value: 42.5 }]);
    });


    it('table timestamp', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value timestamp);
                                    insert into test(value) values ('2000-01-02 03:04:05');
                                    select * from test;`);
        expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
        expect(many.map(x => moment(x.value)?.format('YYYY-MM-DD HH:mm:ss'))).to.deep.equal(['2000-01-02 03:04:05']);
    });

    // TODO =>  NOT SUPPORTED BY PARSER
    // it('table timestamp with time zone', () => {
    //     const db = newDb();
    //     const many = db.query.many(`create table test(value timestamp with time zone);
    //                                 insert into test(value) values ('2000-01-02 03:04:05');
    //                                 select * from test;`);
    //     expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
    //     expect(many.map(x => moment(x.value)?.format('YYYY-MM-DD HH:mm:ss'))).to.deep.equal(['2000-01-02 03:04:05']);
    // });

    it('table date', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value date);
                                    insert into test(value) values ('2000-01-02 03:04:05');
                                    select * from test;`);
        expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
        expect(many.map(x => moment(x.value)?.format('YYYY-MM-DD HH:mm:ss'))).to.deep.equal(['2000-01-02 00:00:00']);
    });


    // TODO =>  NOT SUPPORTED BY PARSER
    // it('table date array', () => {
    //     const db = newDb();
    //     const many = db.query.many(`create table test(value date[]);
    //                                 insert into test(value) values ('{2000-01-02 03:04:05,2000-01-02 03:04:05');
    //                                 select * from test;`);
    //     expect(many.map(x => x.value instanceof Date)).to.deep.equal([true]);
    //     expect(many.map(x => x.value?.map?.(y => moment(y)?.format('YYYY-MM-DD HH:mm:ss')))).to.deep.equal([['2000-01-02 00:00:00', '2000-01-02 00:00:00']]);
    // });

    it('table jsonb', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value jsonb);
                                    insert into test(value) values ('{"a": 42}');
                                    select * from test;`);
        expect(many).to.deep.equal([{ value: { a: 42 } }]);
    });


    it('table json', () => {
        const db = newDb();
        const many = db.public.many(`create table test(value json);
                                    insert into test(value) values ('{"a": 42}');
                                    select * from test;`);
        expect(many).to.deep.equal([{ value: { a: 42 } }]);
    });

    it('table invalid jsonb', () => {
        const db = newDb();
        db.public.none(`create table test(value jsonb);`);
        assert.throws(() => db.public.none(`insert into test(value) values ('{"a" 42}');`));
    });
});
