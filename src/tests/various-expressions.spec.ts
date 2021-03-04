import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { expectSingle } from './test-utils';

describe('Various expressions', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    // https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT
    describe('EXTRACT()', () => {

        expectSingle(`SELECT EXTRACT(CENTURY FROM TIMESTAMP '2000-12-16 12:21:13')`, 20);
        expectSingle(`SELECT EXTRACT(CENTURY FROM TIMESTAMP '2001-02-16 20:38:40')`, 21);

        expectSingle(`SELECT EXTRACT(DECADE FROM TIMESTAMP '2001-02-16 20:38:40')`, 200);

        expectSingle(`SELECT EXTRACT(DAY FROM TIMESTAMP '2001-02-16 20:38:40')`, 16);

        expectSingle(`SELECT EXTRACT(DAY FROM INTERVAL '40 days 1 minute')`, 40);
        expectSingle(`SELECT EXTRACT(DAY FROM INTERVAL '1 year 40 days 1 minute')`, 40);

        expectSingle(`SELECT EXTRACT(DOW FROM TIMESTAMP '2001-02-16 20:38:40');`, 5);

        expectSingle(`SELECT EXTRACT(DOY FROM TIMESTAMP '2001-02-16 20:38:40');`, 47);

        expectSingle(`SELECT EXTRACT(EPOCH FROM TIMESTAMP WITH TIME ZONE '2001-02-16 20:38:40.12-08');`, 982384720);


        expectSingle(`SELECT EXTRACT(EPOCH FROM INTERVAL '5 days 3 hours');`, 442800);

        expectSingle(`SELECT EXTRACT(HOUR FROM TIMESTAMP '2001-02-16 20:38:40');`, 20);

        expectSingle(`SELECT EXTRACT(hour FROM INTERVAL '5 days 3.5 hours');`, 3);

        expectSingle(`SELECT EXTRACT(ISODOW FROM TIMESTAMP '2001-02-18 20:38:40')`, 7);

        expectSingle(`SELECT EXTRACT(ISOYEAR FROM DATE '2006-01-01')`, 2005);
        expectSingle(`SELECT EXTRACT(ISOYEAR FROM DATE '2006-01-02')`, 2006);

        expectSingle(`SELECT EXTRACT(MICROSECONDS FROM INTERVAL '1 month 28 seconds 500 milliseconds')`, 28500000);
        expectSingle(`SELECT EXTRACT(MICROSECONDS FROM TIME '17:12:28')`, 28000000);
        expectSingle(`SELECT EXTRACT(MICROSECONDS FROM TIME '17:12:28.5')`, 28500000);

        expectSingle(`SELECT EXTRACT(MILLENNIUM FROM TIMESTAMP '2001-02-16 20:38:40')`, 3);

        expectSingle(`SELECT EXTRACT(MILLISECONDS FROM TIME '17:12:28.5')`, 28500);
        expectSingle(`SELECT EXTRACT(MILLISECONDS FROM INTERVAL '10 min 1 seconds 500 milliseconds')`, 1500);

        expectSingle(`SELECT EXTRACT(MINUTE FROM TIMESTAMP '2001-02-16 20:38:40')`, 38);

        expectSingle(`SELECT EXTRACT(MONTH FROM TIMESTAMP '2001-02-16 20:38:40')`, 2);
        expectSingle(`SELECT EXTRACT(MONTH FROM INTERVAL '2 years 3 months')`, 3);
        expectSingle(`SELECT EXTRACT(MONTH FROM INTERVAL '2 years 13 months')`, 1);

        expectSingle(`SELECT EXTRACT(QUARTER FROM TIMESTAMP '2001-02-16 20:38:40')`, 1);

        expectSingle(`SELECT EXTRACT(SECOND FROM TIMESTAMP '2001-02-16 20:38:40')`, 40);
        expectSingle(`SELECT EXTRACT(second FROM INTERVAL '10 min 1 seconds 500 milliseconds')`, 1.5);
        expectSingle(`SELECT EXTRACT(SECOND FROM TIME '17:12:28.5')`, 28.5);

        expectSingle(`SELECT EXTRACT(WEEK FROM TIMESTAMP '2001-02-16 20:38:40')`, 7);

        expectSingle(`SELECT EXTRACT(YEAR FROM TIMESTAMP '2001-02-16 20:38:40')`, 2001);

        expectSingle(`SELECT EXTRACT(YEAR FROM TIMESTAMPTZ '2001-02-16 20:38:40')`, 2001);

    });


    expectSingle(`select array(select 1)`, [1]);
});