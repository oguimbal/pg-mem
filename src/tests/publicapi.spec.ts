import 'mocha';
import 'chai';
import { expect, assert } from 'chai';
import { newDb } from '../db';
import { IMemoryDb, QueryError } from '../interfaces';
import { cleanResults } from '../execution/clean-results';

describe('Test utils', () => {

    let db: IMemoryDb;
    let many: (str: string) => any[];
    function all(table = 'data') {
        return many(`select * from ${table}`);
    }

    beforeEach(() => {
        db = newDb();
        many = db.public.many.bind(db.public);
    });

    interface R {
        id: string;
        dt: Date;
        obj: any;
        i: number;
        n: number | null;
    }
    function simple() {
        many(`create table test(id text unique not null, dt timestamp, obj jsonb, i int, n float default 0)`);
        return db.getTable<R>('test');
    }

    it('matches constraints', () => {
        const table = simple();
        assert.throws(() => table.insert({}), /null value in column "id" violates not-null constraint/);
    })

    it('cannot insert twice', () => {
        const table = simple();
        table.insert({ id: 'x' });
        expect(() => table.insert({ id: 'x' })).to.throw().that.satisfies((e: QueryError) => e.data.code === '23505')
    })

    it('sets defaults', () => {
        const table = simple();
        const [got] = cleanResults([table.insert({ id: 'x' })]);
        expect(got).to.deep.equal({
            id: 'x',
            obj: null,
            dt: null,
            i: null,
            n: 0,
        })
    });


    it('does not set default when explicitely null', () => {
        const table = simple();
        const [got] = cleanResults([table.insert({ id: 'x', })]);
        expect(got).to.deep.equal({
            id: 'x',
            obj: null,
            dt: null,
            i: null,
            n: 0,
        })
    });

    it('can find all', () => {
        const table = simple();
        table.insert({ id: 'a' })
        table.insert({ id: 'b' });
        table.insert({ id: 'c' });

        const got = [...table.find(null, ['id'])];

        expect(got).to.deep.equal([
            { id: 'a' },
            { id: 'b' },
            { id: 'c' },
        ]);
    });

    it('can find template', () => {

        const table = simple();
        table.insert({ id: 'a', n: 1 })
        table.insert({ id: 'b', n: 42 });
        table.insert({ id: 'c', n: 42 });
        table.insert({ id: 'd', n: 42, i: 51 });
        table.insert({ id: 'e', n: 42, i: 51 });

        const got = [...table.find({ n: 42, i: 51 }, ['id'])];

        expect(got).to.deep.equal([
            { id: 'd' },
            { id: 'e' },
        ]);
    })


    it('accepts null as template entry', () => {
        const table = simple();
        table.insert({ id: 'a' })
        table.insert({ id: 'b', n: 42 });
        table.insert({ id: 'c', n: 42 });
        table.insert({ id: 'd', n: null })

        const got = [...table.find({ n: null }, ['id'])];

        expect(got).to.deep.equal([
            { id: 'd' },
        ]);
    })


    it('copies input', () => {
        const table = simple();
        const orig: Partial<R> = { id: 'id', obj: { sub: 42 }, n: 42 };
        const check = () => expect(cleanResults([...table.find()])).to.deep.equal([{
            id: 'id',
            obj: { sub: 42 },
            i: null,
            dt: null,
            n: 42
        }]);
        const mutate = (t: Partial<R>) => {
            t.n = 51;
            t.obj.sub = 55;
        }
        // insert
        const inserted = table.insert(orig);
        if (inserted == null) {
            assert(false);
            return; // satisfy typescript
        }

        // mutate original + check query result not impacted
        mutate(orig);
        check();

        // mutate inserted + check query result not impacted
        mutate(inserted);
        check();

        // mutate query result + check query result not impacted
        mutate([...table.find()][0]);
        check();

    })
});
