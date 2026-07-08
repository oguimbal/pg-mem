import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';
import { IMemoryDb } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('JSON functions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let one: (str: string) => any;
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        one = db.public.one.bind(db.public);
    });

    it('#>> extracts text at path', () => {
        // nb: "#>" is not in the parser grammar yet; jsonb_extract_path covers it
        expect(one(`select '{"a":{"b":1}}'::jsonb #>> '{a,b}' as v`).v).toBe('1');
        expect(one(`select '{"a":[10,20]}'::jsonb #>> '{a,1}' as v`).v).toBe('20');
        expect(one(`select '{"a":1}'::jsonb #>> '{nope,x}' as v`).v).toBeNull();
    });

    it('json_build_object', () => {
        expect(one(`select json_build_object('a', 1, 'b', 'x') as v`).v).toEqual({ a: 1, b: 'x' });
        expect(one(`select jsonb_build_object('n', null) as v`).v).toEqual({ n: null });
        expectQueryError(() => one(`select json_build_object('a')`), /even number/);
    });

    it('json_build_array', () => {
        expect(one(`select jsonb_build_array(1, 'x', null) as v`).v).toEqual([1, 'x', null]);
    });

    it('jsonb_array_length', () => {
        expect(one(`select jsonb_array_length('[1,2,3]'::jsonb) as v`).v).toBe(3);
        expectQueryError(() => one(`select jsonb_array_length('{"a":1}'::jsonb)`), /non-array/);
    });

    it('jsonb_set', () => {
        expect(one(`select jsonb_set('{"a":1}'::jsonb, '{a}', '2'::jsonb) as v`).v).toEqual({ a: 2 });
        expect(one(`select jsonb_set('{"a":{"b":1}}'::jsonb, '{a,b}', '"x"'::jsonb) as v`).v)
            .toEqual({ a: { b: 'x' } });
        expect(one(`select jsonb_set('{"a":1}'::jsonb, '{new}', '5'::jsonb) as v`).v).toEqual({ a: 1, new: 5 });
        // createMissing = false
        expect(one(`select jsonb_set('{"a":1}'::jsonb, '{new}', '5'::jsonb, false) as v`).v).toEqual({ a: 1 });
        // array index set
        expect(one(`select jsonb_set('[1,2,3]'::jsonb, '{1}', '9'::jsonb) as v`).v).toEqual([1, 9, 3]);
    });

    it('to_jsonb', () => {
        expect(one(`select to_jsonb('txt'::text) as v`).v).toBe('txt');
        expect(one(`select to_jsonb(42) as v`).v).toBe(42);
    });

    it('jsonb_typeof', () => {
        expect(many(`select jsonb_typeof('[1]'::jsonb) as a, jsonb_typeof('{"x":1}'::jsonb) as b,
                            jsonb_typeof('"s"'::jsonb) as c, jsonb_typeof('1'::jsonb) as d,
                            jsonb_typeof('true'::jsonb) as e, jsonb_typeof('null'::jsonb) as f`))
            .toEqual([{ a: 'array', b: 'object', c: 'string', d: 'number', e: 'boolean', f: 'null' }]);
    });

    it('jsonb_strip_nulls', () => {
        expect(one(`select jsonb_strip_nulls('{"a":1,"b":null,"c":{"d":null,"e":2}}'::jsonb) as v`).v)
            .toEqual({ a: 1, c: { e: 2 } });
    });

    it('jsonb_extract_path and _text', () => {
        expect(one(`select jsonb_extract_path('{"a":{"b":[5]}}'::jsonb, 'a', 'b') as v`).v).toEqual([5]);
        expect(one(`select jsonb_extract_path_text('{"a":{"b":[5]}}'::jsonb, 'a', 'b') as v`).v).toBe('[5]');
    });

    it('jsonb_each in FROM yields key/value rows', () => {
        expect(many(`select * from jsonb_each('{"a":1,"b":"x"}'::jsonb)`))
            .toEqual([{ key: 'a', value: 1 }, { key: 'b', value: 'x' }]);
        expect(many(`select * from jsonb_each_text('{"a":{"b":1}}'::jsonb)`))
            .toEqual([{ key: 'a', value: '{"b":1}' }]);
    });

    it('jsonb_object_keys in FROM yields one row per key', () => {
        expect(many(`select * from jsonb_object_keys('{"a":1,"b":2}'::jsonb) as k`))
            .toEqual([{ k: 'a' }, { k: 'b' }]);
    });

    it('jsonb_array_elements in FROM', () => {
        expect(many(`select * from jsonb_array_elements('[1,"x"]'::jsonb) as e`))
            .toEqual([{ e: 1 }, { e: 'x' }]);
    });
});
