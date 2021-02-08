import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { DataType, IMemoryDb } from '../interfaces';
import { preventSeqScan } from './test-utils';

describe('Functions', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    let none: (str: string) => void;
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }
    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
        none = db.public.none.bind(db.public);
    });


    it('does not pay attention to casing', () => {
        expect(many(`SELECT CONCAT('a', 'b', 'c')`))
            .to.deep.equal([{ concat: 'abc' }]);
        expect(many(`SELECT ConCat('a', 'b', 'c')`))
            .to.deep.equal([{ concat: 'abc' }]);
        assert.throws(() => many(`SELECT "ConCat"('a', 'b', 'c')`), /does not exist/);
    })

    it('can declare & call function', () => {
        db.registerLanguage('mylang', ({ code, args, returns }) => {
            expect(code).to.equal('some code');
            expect(args.map(x => x.type.primary)).to.deep.equal([DataType.text]);
            expect(returns?.primary).to.equal(DataType.text);
            return arg => {
                return 'hello ' + arg;
            }
        });

        none(`CREATE FUNCTION "sayHello"(in arg text) RETURNS text
        AS $$some code$$
        LANGUAGE mylang`);

        assert.throws(() => many(`select sayHello('world')`), /does not exist/);

        expect(many(`select "sayHello"('world')`))
            .to.deep.equal([{ sayHello: 'hello world' }]);
    });
});
