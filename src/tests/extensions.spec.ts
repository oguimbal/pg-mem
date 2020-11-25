import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';
import { DataType } from '../interfaces';

describe('Extensions', () => {

    let db: _IDb;
    let many: (str: string) => any[];
    beforeEach(() => {
        db = newDb() as _IDb;
        many = db.public.many.bind(db.public);
    });

    it ('can call function declared in extension', () => {
        db.registerExtension('ext', s => s.registerFunction({
            name: 'say_hello',
            args: [DataType.text],
            returns: DataType.text,
            implementation: x => 'hello ' + x,
        }));

        expect(many(`create extension ext;
                    select say_hello('world') as msg`))
        .to.deep.equal([{
            msg: 'hello world',
        }])
    })


    it ('can call function declared in another schema', () => {
        db.getSchema('pg_catalog').registerFunction({
            name: 'col_description',
            args: [DataType.int, DataType.int],
            returns: DataType.text,
            implementation: x => 'nothing',
        });

        expect(many(`select pg_catalog.col_description(1,2) as msg`))
        .to.deep.equal([{
            msg: 'nothing',
        }])
    })
});