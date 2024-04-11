import 'mocha';
import 'chai';
import { expect } from 'chai';
import { newDb } from '../db';
import postgres from 'postgres';

// Spin up a real postgres instance with e.g., docker,
// to compare the results of the fake postgres instance
// with the real one.
const realSQL = postgres({
    host: 'localhost',
    port: 5432,
    database: 'postgres',
    username: 'postgres',
    password: 'password',
})

describe.only("PostgresJS smoketests", () => {
    it("should use template correctly", async () => {
        const mem = newDb();
        const pg = mem.adapters.createPostgresJs();
        const fake = await doSQLStuff(pg);

        await realSQL`DROP TABLE IF EXISTS users`; // Clear table from any previous tests
        const real = await doSQLStuff(realSQL);

        expect(fake).to.deep.equal(real);
    })


    it("should use unsafe correctly", async () => {
        const mem = newDb();
        const pg = mem.adapters.createPostgresJs();
        const fake = await doUnsafeStuff(pg);

        await realSQL`DROP TABLE IF EXISTS users`;
        const real = await doUnsafeStuff(realSQL);

        expect(fake).to.deep.equal(real); // [ { id: 1, name: 'Alice' }]
    });
});

async function doSQLStuff(sql: any) {
    // Use the postgres-js tagged template literal to run SQL queries
    await sql`
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    )`;
    await sql`INSERT INTO users (name) VALUES ('Alice')`;
    return await sql`SELECT * FROM users`;
}

async function doUnsafeStuff(sql: any) {
    await sql.unsafe(`
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name TEXT NOT NULL
    )`)
    await sql.unsafe(`INSERT INTO users (name) VALUES ('Alice')`);
    return await sql.unsafe(`SELECT * FROM users`);
}
