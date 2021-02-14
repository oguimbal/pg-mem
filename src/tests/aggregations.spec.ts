import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Aggregations', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
        one = db.public.one.bind(db.public);
    });


    it('supports max()', () => {
        expect(many(`create table example(a int);
                    insert into example values (3), (null), (1), (5), (2);
                    select max(a) from example`))
            .to.deep.equal([{ max: 5 }]);
    })

    it('max() returns null when nothing', () => {
        expect(many(`create table example(a int);
                    select max(a) from example`))
            .to.deep.equal([{ max: null }]);
    })


    it('supports min()', () => {
        expect(many(`create table example(a int);
                    insert into example values (3), (null), (1), (5), (2);
                    select min(a) from example`))
            .to.deep.equal([{ min: 1 }]);
    })

    it('min() returns null when nothing', () => {
        expect(many(`create table example(a int);
                    select min(a) from example`))
            .to.deep.equal([{ min: null }]);
    })

    it('supports sum()', () => {
      expect(many(`create table example(a int);
                  insert into example values (1), (-2), (null), (3), (-4), (5);
                  select sum(a) from example`))
            .to.deep.equal([{ sum: 3 }])
    })

    it('sum() returns null when nothing', () => {
      expect(many(`create table example(a int);
                  select sum(a) from example`))
            .to.deep.equal([{ sum: null }]);
  })

  it('supports sum(distinct())', () => {
    expect(many(`create table example(a int);
                insert into example values (1), (1), (null), (2), (2), (3), (-1), (-1);
                select sum(distinct(a)) from example`))
            .to.deep.equal([{ sum: 5 }])
  })

  it('sum(distinct()) returns null when nothing', () => {
    expect(many(`create table example(a int);
                select sum(distinct(a)) from example`))
            .to.deep.equal([{ sum: null }]);
  })
});
