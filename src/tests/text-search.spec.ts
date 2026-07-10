import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';

describe('full-text search', () => {

    let db: IMemoryDb;
    let one: (str: string) => any;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    beforeEach(() => {
        db = newDb();
        one = db.public.one.bind(db.public);
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });

    it('to_tsvector with simple config keeps all tokens, lowercased, with positions', () => {
        expect(one(`select to_tsvector('simple', 'The Quick Brown Foxes') as v`).v)
            .toEqual(`'brown':3 'foxes':4 'quick':2 'the':1`);
    });

    it('to_tsvector english stems and drops stop-words', () => {
        expect(one(`select to_tsvector('english', 'The Quick Brown Foxes jumping running') as v`).v)
            .toEqual(`'brown':3 'fox':4 'jump':5 'quick':2 'run':6`);
    });

    it('defaults to the english config', () => {
        expect(one(`select to_tsvector('the running dogs') as v`).v)
            .toEqual(`'dog':3 'run':2`);
    });

    it('lists repeated lexeme positions', () => {
        expect(one(`select to_tsvector('simple', 'the cat the') as v`).v)
            .toEqual(`'cat':2 'the':1,3`);
    });

    it('plainto_tsquery ANDs stemmed tokens', () => {
        expect(one(`select plainto_tsquery('english', 'jumping foxes') as q`).q)
            .toEqual(`'jump' & 'fox'`);
    });

    it('to_tsquery keeps operators', () => {
        expect(one(`select to_tsquery('simple', 'quick & brown') as q`).q)
            .toEqual(`'quick' & 'brown'`);
    });

    it('@@ matches with stemming (english)', () => {
        expect(one(`select to_tsvector('english', 'running') @@ to_tsquery('english', 'run') as m`).m).toEqual(true);
        expect(one(`select to_tsvector('simple', 'running') @@ to_tsquery('simple', 'run') as m`).m).toEqual(false);
    });

    it('@@ evaluates boolean queries', () => {
        expect(one(`select to_tsvector('simple', 'a b c') @@ to_tsquery('simple', 'a & b') as m`).m).toEqual(true);
        expect(one(`select to_tsvector('simple', 'a b c') @@ to_tsquery('simple', 'a & z') as m`).m).toEqual(false);
        expect(one(`select to_tsvector('simple', 'a b c') @@ to_tsquery('simple', 'z | b') as m`).m).toEqual(true);
        expect(one(`select to_tsvector('simple', 'a b c') @@ to_tsquery('simple', '!z') as m`).m).toEqual(true);
    });

    it('filters a table with a full-text predicate', () => {
        none(`create table docs (id int, body text);
               insert into docs values
                 (1, 'the quick brown fox'),
                 (2, 'lazy dogs sleeping'),
                 (3, 'a fox jumped over')`);
        expect(many(`select id from docs
                     where to_tsvector('english', body) @@ plainto_tsquery('english', 'foxes')
                     order by id`))
            .toEqual([{ id: 1 }, { id: 3 }]);
    });

    it('stores a tsvector column', () => {
        none(`create table d (id int, v tsvector);
               insert into d values (1, to_tsvector('simple', 'hello world'))`);
        expect(one(`select v from d where id = 1`).v).toEqual(`'hello':1 'world':2`);
        expect(one(`select id from d where v @@ to_tsquery('simple', 'world') limit 1`).id).toEqual(1);
    });
});
