import { newDb } from '../.deno/mod.ts';

const db = newDb();

db.public.none(`create table test(id text);
                insert into test values ('value');`);
// create a restore point & mess with data
const backup = db.backup();
db.public.none(`update test set id='new value';`)
// restore it !
backup.restore();
console.log(db.public.many(`select * from test`)) // => {test: 'value'}