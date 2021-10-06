import 'mocha';
import 'chai';
import { newDb } from '../db';
import { expect, assert } from 'chai';
import { _IDb } from '../interfaces-private';

describe('Subqueries', () => {

        let db: _IDb;
        let many: (str: string) => any[];
        let none: (str: string) => void;

        beforeEach(() => {
                db = newDb() as _IDb;
                many = db.public.many.bind(db.public);
                none = db.public.none.bind(db.public);
        });

        function setupBooks() {
                none(`create table books(name text, created_at int);
                insert into books values ('one', 1), ('two', 2), ('three', 3), ('four', 1);
                `);
        }

        it('[bugfix] can select select = subqueries with min()', () => {
                // checks https://github.com/oguimbal/pg-mem/issues/162
                setupBooks();
                expect(many(`SELECT name FROM books WHERE created_at = (SELECT MIN(created_at) FROM books);`))
                        .to.deep.equal([{ name: 'one' }, { name: 'four' }]);
        });

        it('can = subquery when one resulting row', () => {
                setupBooks();
                expect(many(`SELECT name FROM books WHERE created_at = (SELECT created_at FROM books WHERE created_at > 2)`))
                        .to.deep.equal([{ name: 'three' }]);
        });

        it('cannot = subquery when multiple resulting row', () => {
                setupBooks();
                assert.throws(() => none(`SELECT name FROM books WHERE created_at = (SELECT created_at FROM books WHERE created_at < 2)`), /more than one row returned by a subquery used as an expression/);
        });

        it('cannot = subquery when multiple columns', () => {
                setupBooks();
                assert.throws(() => none(`SELECT name FROM books WHERE created_at = (SELECT created_at, name FROM books WHERE created_at < 2)`), /subquery must return only one column/);
        });


        function mytable() {
                many(`CREATE TABLE my_table (id text NOT NULL PRIMARY KEY, name text NOT NULL, parent_id text);
                CREATE INDEX my_table_idx_name ON my_table (name);
                CREATE INDEX my_table_idx_id_parent_id ON my_table (id,parent_id);

                insert into my_table values ('parid', 'Parent', null);
                insert into my_table values ('childid', 'Child', 'parid');`);
        }


        describe.skip('With subqueries accessing parent scope', () => {
                it('fails if multiple columns in predicate', () => {
                        mytable();
                        assert.throws(() => many(`SELECT name FROM my_table as t1 WHERE id = (SELECT name, id FROM my_table as t2 WHERE t2.parent_id = t1.id);`), /subquery must return only one column/);
                });

                it('fails if multiple columns in selection', () => {
                        mytable();
                        assert.throws(() => many(`SELECT name, (SELECT name FROM my_table as t2 WHERE t2.parent_id = t1.id) FROM my_table as t1`), /subquery must return only one column/);
                });

                it('supports self aliasing (bugfix)', () => {
                        mytable();
                        expect(many(`SELECT name FROM my_table as t1 WHERE NOT EXISTS (SELECT * FROM my_table as t2 WHERE t2.parent_id = t1.id);`))
                                .to.deep.equal([{ name: 'Child' }]);
                });
        });


        // it('simplifies a subquery when possible', () => {
        //     mytable();
        //     let cnt = 0;
        //     db.on('subquery', () => {
        //         cnt++
        //     });
        //     db.on('non-constant-subquery', () => {
        //         assert.fail('Should not have raised non-constant-subquery');
        //     })
        //     expect(many(`SELECT name FROM my_table as t1 WHERE id = (SELECT id FROM my_table LIMIT 1)`))
        //         .to.deep.equal([{ name: 'Parent' }]);

        //     expect(cnt).to.equal(1, 'Was expecting subquery to be simplified');
        // })
});
