import { describe, it, beforeEach, expect } from 'bun:test';

import { newDb } from '../db';

import { _IDb } from '../interfaces-private';
import { DataType } from '../interfaces';
import { expectQueryError } from './test-utils';

describe('Extensions', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
    });

    it('can call function declared in extension', () => {
        db.registerExtension('ext', s => s.registerFunction({
            name: 'say_hello',
            args: [DataType.text],
            returns: DataType.text,
            implementation: x => 'hello ' + x,
        }));

        expect(many(`create extension ext;
                    select say_hello('world') as msg`))
            .toEqual([{
                msg: 'hello world',
            }])
    })


    it('can call function declared in another schema', () => {
        expect(many(`select pg_catalog.col_description(1,2) as msg`))
            .toEqual([{
                msg: 'Fake description provided by pg-mem',
            }])
    });


    it('cannot create extension twice', () => {
        db.registerExtension('ext', s => { });

        many('create extension "ext"');
        expectQueryError(() => many('create extension "ext"'));
    });

    it('can recreate extension twice with "if not exists"', () => {
        db.registerExtension('ext', s => { });

        many('create extension if not exists "ext"');
        many('create extension if not exists "ext"');
    });
});