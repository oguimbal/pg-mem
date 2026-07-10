// A representative pg-mem workload, run identically against two builds so we can compare
// the fork against upstream. Exercises the hot paths: DDL, bulk insert, indexed lookups,
// sequential scans, joins, aggregation, updates - the things pg-mem is meant to be fast at.

export function runWorkload(newDb) {
    const ROWS = 5000;
    const timings = {};
    const time = (name, fn) => {
        const t0 = performance.now();
        fn();
        timings[name] = performance.now() - t0;
    };

    const db = newDb();
    const none = db.public.none.bind(db.public);
    const many = db.public.many.bind(db.public);

    time('create schema', () => {
        none(`create table users (id int primary key, name text, age int, active boolean);
              create table orders (id int primary key, user_id int, total int, note text);
              create index orders_user on orders(user_id);`);
    });

    time('insert 5k users', () => {
        for (let i = 0; i < ROWS; i++) {
            none(`insert into users values (${i}, 'user_${i}', ${20 + (i % 50)}, ${i % 2 === 0})`);
        }
    });

    time('insert 5k orders', () => {
        for (let i = 0; i < ROWS; i++) {
            none(`insert into orders values (${i}, ${i % ROWS}, ${(i * 7) % 1000}, 'note ${i}')`);
        }
    });

    time('pk lookups x2000', () => {
        for (let i = 0; i < 2000; i++) {
            many(`select * from users where id = ${(i * 3) % ROWS}`);
        }
    });

    time('indexed fk lookups x2000', () => {
        for (let i = 0; i < 2000; i++) {
            many(`select * from orders where user_id = ${(i * 3) % ROWS}`);
        }
    });

    time('seq scan filter x200', () => {
        for (let i = 0; i < 200; i++) {
            many(`select id, name from users where age > 40 and active = true`);
        }
    });

    time('join x200', () => {
        for (let i = 0; i < 200; i++) {
            many(`select u.name, o.total from users u join orders o on o.user_id = u.id where u.id = ${(i * 5) % ROWS}`);
        }
    });

    time('aggregation x100', () => {
        for (let i = 0; i < 100; i++) {
            many(`select user_id, count(*) as c, sum(total) as s from orders group by user_id`);
        }
    });

    time('updates x2000', () => {
        for (let i = 0; i < 2000; i++) {
            none(`update users set age = age + 1 where id = ${(i * 3) % ROWS}`);
        }
    });

    time('arithmetic x5000', () => {
        for (let i = 0; i < 5000; i++) {
            many(`select ${i} + ${i} as a, ${i} * 2 as b, ${i} / 3 as c`);
        }
    });

    return timings;
}
