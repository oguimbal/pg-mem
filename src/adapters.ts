import { LibAdapters, IMemoryDb, NotSupported, QueryResult } from './interfaces';
import { literal } from './pg-escape';
import moment from 'moment';
import lru from 'lru-cache';
declare var __non_webpack_require__;

const delay = (time: number) => new Promise(done => setTimeout(done, time));

function replaceQueryArgs$(this: void, sql: string, values: any[]) {
    return sql.replace(/\$(\d+)/g, (str, istr) => {
        const i = Number.parseInt(istr);
        if (i > values.length) {
            throw new Error('Unmatched parameter in query ' + str);
        }
        const val = values[i - 1];
        switch (typeof val) {
            case 'string':
                return literal(val);
            case 'boolean':
                return val ? 'true' : 'false';
            case 'number':
                return val.toString(10);
            default:
                if (val === null || val === undefined) {
                    return null;
                }
                if (val instanceof Date) {
                    return `'${moment(val).toISOString()}'`;
                }
                if (typeof val === 'object') {
                    return literal(JSON.stringify(val));
                }
                throw new Error('Invalid query parameter')
        }
    });
}

export class Adapters implements LibAdapters {

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
            on() {
                // nop
            }
            release() {
            }
            removeListener() {
            }
            end(callback) {
                callback();
                return Promise.resolve();
            }
            connect(callback) {
                callback?.(null, this, () => { });
                return Promise.resolve(this);
            }
            query(query, valuesOrCallback, callback) {
                let values = null;
                if (Array.isArray(valuesOrCallback)) {
                    values = valuesOrCallback;
                }
                callback = typeof callback === 'function'
                    ? callback
                    : valuesOrCallback;

                const pgquery = this.adaptQuery(query, values);

                return new Promise((done, err) => setTimeout(() => {
                    try {
                        const result = this.adaptResults(query, that.db.public.query(pgquery.text));
                        callback?.(null, result)
                        done(result);
                    } catch (e) {
                        callback(e);
                        err(e);
                    }
                }, queryLatency));
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

            private adaptQuery(query: string | PgQuery, values: any[]): PgQuery {
                if (typeof query === 'string') {
                    query = {
                        text: query,
                        values,
                    };
                } else {
                    // clean copy to avoid mutating things outside our scope
                    query = { ...query };
                }
                if (!values?.length) {
                    return query;
                }

                if (query.types?.getTypeParser) {
                    throw new NotSupported('getTypeParser is not supported');
                }

                // console.log(query);
                // console.log('\n');

                query.text = replaceQueryArgs$(query.text, values);
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


    createPgPromise(queryLatency?: number) {
        // https://vitaly-t.github.io/pg-promise/module-pg-promise.html
        const pgp = __non_webpack_require__('@oguimbal/pg-promise')({
            customPg: this.createPg(queryLatency),
        });
        return pgp('fake connection string');
    }

    createPgNative(queryLatency?: number) {
        queryLatency = queryLatency ?? 0;
        const prepared = new lru<string, string>({
            max: 1000,
            maxAge: 5000,
        });
        function handlerFor(a, b) {
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

}