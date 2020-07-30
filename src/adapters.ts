import { LibAdapters, IMemoryDb, NotSupported, QueryResult } from './interfaces';
import { literal } from './pg-escape';
import moment from 'moment';
declare var __non_webpack_require__;

export class Adapters implements LibAdapters {

    constructor(private db: IMemoryDb) {
    }

    createPg(queryLatency?: number) {
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

                query.text = query.text.replace(/\$(\d+)/g, (str, istr) => {
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
        throw new Error('Method not implemented.');
    }

}