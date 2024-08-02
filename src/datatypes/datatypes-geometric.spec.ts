import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { _ITable } from '../interfaces-private';

describe('Datatypes - geometric', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
    });


    it('can select points', () => {
        expect(many(`select '1,2'::point`))
            .toEqual([{ point: { x: 1, y: 2 } }])
    })

    it('should return copies', () => {
        const [{ pt }] = many(`create table test(pt point);
                                insert into test values ('1,2');
                                select * from test`);
        expect(pt).toEqual({ x: 1, y: 2 })

        // modify it
        pt.x = 42;

        // check immutability
        expect(many(`select * from test`))
            .toEqual([{ pt: { x: 1, y: 2 } }])
    })

    it('can convert points back to text', () => {
        expect(many(`select '1,2'::point::text`))
            .toEqual([{ text: '(1,2)' }])
    })

    it('supports path equality, which always return true', () => {
        expect(many(`select '1,2,3,4'::path = '1,2,3,4'::path as eq`))
            .toEqual([{ eq: true }]);
        expect(many(`select '55,2,3,4'::path = '3,5,55,4'::path as eq`))
            .toEqual([{ eq: true }]); // YES! "TRUE" ! Try it... that always returns true.
    });

    it('supports path strict inequalitites, which always return false', () => {
        expect(many(`select '1,2,3,4'::path < '1,2,3,4'::path as eq`))
            .toEqual([{ eq: false }]);
    });

    it('supports path inequalitites, which always return true', () => {
        expect(many(`select '1,2,3,4'::path <= '1,2,3,4'::path as eq`))
            .toEqual([{ eq: true }]);
    });


    describe.skip('Todo', () => {
        it('can equate points to text', () => {
            expect(() => many(`select '1,2'::point = '1,2'`)).toThrow(/operator does not exist: point = unknown/);
        })

        it('does not support point equality', () => {
            expect(() => many(`select '1,2'::point = '1,2'::point`)).toThrow(/operator does not exist: point = point/)
        });

        it('does not support point inequality', () => {
            expect(() => many(`select '1,2'::point < '1,2'::point`)).toThrow(/operator does not exist: point < point/)
        });
    })
});
