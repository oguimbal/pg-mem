import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { preventSeqScan } from './test-utils';

describe('Overlaps', () => {
  
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

    it('select result of conditional overlaps expression dates', () => {
        expect(
            one(`SELECT (
                DATE '2016-01-10', DATE '2016-02-01'
              ) OVERLAPS (
                DATE '2016-01-20', DATE '2016-02-10'
              );`
            )
        )
        .to.equal({ overlaps: true });
    });

    it('select result of conditional overlaps expression date times', () => {
      expect(
          one(
            `SELECT (
              timestamp '2022-11-23 03:00:00.000+00', timestamp '2022-11-24 03:00:01.000+00'
            ) OVERLAPS (
              timestamp '2022-11-24 03:00:00.000+00', timestamp '2022-11-25 03:00:00.000+00'
            );`
          )
      )
      .to.equal({ overlaps: true });
    });

    it('select result of conditional overlaps expression date times', () => {
      expect(
          one(
            `SELECT (
              timestamp '2022-11-23 03:00:00.000+00', timestamp '2022-11-24 03:00:00.000+00'
            ) OVERLAPS (
              timestamp '2022-11-24 03:00:00.000+00', timestamp '2022-11-25 03:00:00.000+00'
            );`
          )
      )
      .to.equal({ overlaps: false });
    });
});
