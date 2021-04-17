import { LibAdapters, IMemoryDb, NotSupported, QueryResult } from './interfaces.ts';
import { literal } from './pg-escape.ts';
import moment from 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts';
import lru from 'https://deno.land/x/lru_cache@6.0.0-deno.4/mod.ts';
import { bufToString, isBuf } from './buffer-deno.ts';
import { compareVersions, nullIsh } from './utils.ts';
declare var __non_webpack_require__: any;

const delay = (time: number | undefined) => new Promise(done => setTimeout(done, time ?? 0));

function replaceQueryArgs$(this: void, sql: string, values: any[]) {
    return sql.replace(/\$(\d+)/g, (str: any, istr: any) => {
        const i = Number.parseInt(istr);
        if (i > values.length) {
            throw new Error('Unmatched parameter in query ' + str);
        }
        const val = values[i - 1];
        return toLiteral(val);
    });
}

function toLiteral(val: any): string {
    switch (typeof val) {
        case 'string':
            return literal(val);
        case 'boolean':
            return val ? 'true' : 'false';
        case 'number':
            return val.toString(10);
        default:
            if (nullIsh(val)) {
                return 'null';
            }
            if (Array.isArray(val)) {
                return `ARRAY[${val.map(x => toLiteral(x)).join(', ')}]`;
            }
            if (val instanceof Date) {
                return `'${moment.utc(val).toISOString()}'`;
            }
            if (isBuf(val)) {
                return literal(bufToString(val));
            }
            if (typeof val === 'object') {
                return literal(JSON.stringify(val));
            }
            throw new Error('Invalid query parameter')
    }
}

export class Adapters implements LibAdapters {
    private _mikroPatched?: boolean;

    constructor(private db: IMemoryDb) {
    }

    createPg(queryLatency?: number): { Pool: any; Client: any } {
        const that = this;
        // https://node-postgres.com/features/queries
        interface PgQuery {
            text: string;
            values?: any[];
            rowMode?: 'array';
            types?: any;
        }
        class MemPg {

            connection = this;

            on() {
                // nop
            }

            release() {
            }

            removeListener() {
            }

            once(what: string, handler: () => void) {
                if (what === 'connect') {
                    setTimeout(handler, queryLatency ?? 0);
                }
            }

            end(callback: any) {
                if (callback) {
                    callback();
                    return null;
                } else {
                    return Promise.resolve();
                }
            }

            connect(callback: any) {
                if (callback) {
                    callback(null, this, () => { });
                    return null;
                } else {
                    return Promise.resolve(this);
                }
            }
            query(query: any, valuesOrCallback: any, callback: any) {
                let values: any = null;
                if (Array.isArray(valuesOrCallback)) {
                    values = valuesOrCallback;
                }
                if (callback == null && typeof valuesOrCallback === 'function') {
                    callback = valuesOrCallback;
                }

                const pgquery = this.adaptQuery(query, values);
                try {
                    const result = this.adaptResults(query, that.db.public.query(pgquery.text));
                    if (callback) {
                        setTimeout(() => callback(null, result), queryLatency ?? 0);
                        return null;
                    } else {
                        return new Promise(res => setTimeout(() => res(result), queryLatency ?? 0));
                    }
                } catch (e) {
                    if (callback) {
                        setTimeout(() => callback(e), queryLatency ?? 0);
                        return null;
                    } else {
                        return new Promise((_, rej) => setTimeout(() => rej(e), queryLatency ?? 0));
                    }
                }
            }

            private adaptResults(query: PgQuery, rows: QueryResult) {
                if (query.rowMode) {
                    throw new NotSupported('pg rowMode')
                }
                return {
                    ...rows,
                    get fields() {
                        throw new NotSupported('get pg fields');
                    }
                }
            }

            private adaptQuery(query: string | PgQuery, values: any): PgQuery {
                if (typeof query === 'string') {
                    query = {
                        text: query,
                        values,
                    };
                } else {
                    // clean copy to avoid mutating things outside our scope
                    query = { ...query };
                }
                if (!query.values?.length) {
                    return query;
                }

                if (query.types?.getTypeParser) {
                    throw new NotSupported('getTypeParser is not supported');
                }

                // console.log(query);
                // console.log('\n');

                query.text = replaceQueryArgs$(query.text, query.values);
                return query;
            }
        }
        return {
            Pool: MemPg,
            Client: MemPg,
        };
    }

    createTypeormConnection(postgresOptions: any, queryLatency?: number) {
        const that = this;
        (postgresOptions as any).postgres = that.createPg(queryLatency);
        if (postgresOptions?.type !== 'postgres') {
            throw new NotSupported('Only postgres supported, found ' + postgresOptions?.type ?? '<null>')
        }

        const { getConnectionManager } = __non_webpack_require__('typeorm')
        const created = getConnectionManager().create(postgresOptions);
        created.driver.postgres = that.createPg(queryLatency);
        return created.connect();
    }

    createSlonik(queryLatency?: number) {
        const { createMockPool, createMockQueryResult } = __non_webpack_require__('slonik');
        return createMockPool({
            query: async (sql: string, args: any[]) => {
                await delay(queryLatency ?? 0);
                const formatted = replaceQueryArgs$(sql, args);
                const ret = this.db.public.many(formatted);
                return createMockQueryResult(ret);
            },
        });
    }


    createPgPromise(queryLatency?: number) {
        // https://vitaly-t.github.io/pg-promise/module-pg-promise.html
        // https://github.com/vitaly-t/pg-promise/issues/743#issuecomment-756110347
        const pgp = __non_webpack_require__('pg-promise')();
        pgp.pg = this.createPg(queryLatency);
        const db = pgp('pg-mem');
        if (compareVersions('10.8.7', db.$config.version) < 0) {
            throw new Error(`💀 pg-mem cannot be used with pg-promise@${db.$config.version},

       👉 you must install version pg-promise@10.8.7 or newer:

                npm i pg-promise@latest -S

            See https://github.com/vitaly-t/pg-promise/issues/743 for details`);
        }
        return db;
    }

    createPgNative(queryLatency?: number) {
        queryLatency = queryLatency ?? 0;
        const prepared = new lru<string, string>({
            max: 1000,
            maxAge: 5000,
        });
        function handlerFor(a: any, b: any) {
            return typeof a === 'function' ? a : b;
        }
        const that = this;
        return class Client {
            async connect(a: any, b: any) {
                const handler = handlerFor(a, b);
                await delay(queryLatency);
                handler?.();
            }

            connectSync() {
                // nop
            }

            async prepare(name: string, sql: string, npar: number, callback: any) {
                await delay(queryLatency);
                this.prepareSync(name, sql, npar);
                callback();
            }

            prepareSync(name: string, sql: string, npar: number) {
                prepared.set(name, sql);
            }

            async execute(name: string, a: any, b: any) {
                const handler = handlerFor(a, b);
                const pars = Array.isArray(a) ? a : [];
                await delay(queryLatency);
                try {
                    const rows = this.executeSync(name, pars);
                    handler(null, rows);
                } catch (e) {
                    handler(e);
                }
            }
            executeSync(name: string, pars?: any) {
                pars = Array.isArray(pars) ? pars : [];
                const prep = prepared.get(name);
                if (!prep) {
                    throw new Error('Unkown prepared statement ' + name);
                }
                return this.querySync(prep, pars);
            }


            async query(sql: string, b: any, c: any) {
                const handler = handlerFor(b, c);
                const params = Array.isArray(b) ? b : [];
                try {
                    await delay(queryLatency);
                    const result = this.querySync(sql, params);
                    handler(null, result);
                } catch (e) {
                    handler?.(e);
                }
            }

            querySync(sql: string, params: any[]) {
                sql = replaceQueryArgs$(sql, params);
                const ret = that.db.public.many(sql);
                return ret;
            }
        }
    }

    createKnex(queryLatency?: number): any {
        const knex = __non_webpack_require__('knex')({
            client: 'pg',
            connection: {},
        });
        knex.client.driver = this.createPg(queryLatency);
        knex.client.version = 'pg-mem';
        return knex;
    }


    async createMikroOrm(mikroOrmOptions: any, queryLatency?: number) {

        const { MikroORM } = __non_webpack_require__('@mikro-orm/core');
        const { AbstractSqlDriver, PostgreSqlConnection, PostgreSqlPlatform } = __non_webpack_require__('@mikro-orm/postgresql');
        const that = this;

        // see https://github.com/mikro-orm/mikro-orm/blob/aa71065d0727920db7da9bfdecdb33e6b8165cb5/packages/postgresql/src/PostgreSqlConnection.ts#L5
        class PgMemConnection extends PostgreSqlConnection {
            protected createKnexClient(type: string) {
                return that.createKnex();
            }

        }
        // see https://github.com/mikro-orm/mikro-orm/blob/master/packages/postgresql/src/PostgreSqlDriver.ts
        class PgMemDriver extends AbstractSqlDriver<PgMemConnection> {
            constructor(config: any) {
                super(config, new PostgreSqlPlatform(), PgMemConnection, ['knex', 'pg']);
            }
        }

        // hack: this query is not supported by pgsql-ast-parser
        if (!this._mikroPatched) {
            this.db.public.interceptQueries(q => {
                if (q === `set names 'utf8';`) {
                    return [];
                }
                console.log(q);
                return null;
            });
            this._mikroPatched = true;
        }


        const orm = await MikroORM.init({
            ...mikroOrmOptions,
            dbName: 'public',
            driver: PgMemDriver,
        });
        return orm;
    }

}
