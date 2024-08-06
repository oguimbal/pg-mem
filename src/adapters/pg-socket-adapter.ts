import { ISchema } from '../interfaces';
import { _IBoundQuery, _IPreparedQuery, _ISchema, _Transaction } from '../interfaces-private';
import { AsyncQueue, delay, doRequire, lazySync, nullIsh } from '../utils';



// https://www.postgresql.org/docs/current/protocol-flow.html
// https://www.postgresql.org/docs/current/protocol-message-formats.html

export const socketAdapter = lazySync(() => {
    const EventEmitter = doRequire('events') as typeof import('events');
    class InMemorySocket extends EventEmitter {
        readonly peer: InMemorySocket;
        isTop: boolean;
        constructor(peer?: InMemorySocket) {
            super();
            this.isTop = !peer;
            this.peer = peer ?? new InMemorySocket(this);
        }

        setNoDelay() { }

        write(data: any, mcb1?: any, mcb2?: any) {
            // if (this.isTop) {
            //     console.log("🛜 ", data);
            // }
            process.nextTick(() => {
                this.peer.emit("data", data);
                mcb1 === "function" && mcb1();
                mcb2 === "function" && mcb2();
            });
            return true;
        }

        end() {
            process.nextTick(() => {
                this.peer.emit("end");
                this.emit("close");
            });
            return this;
        }
        destroySoon() { }
        connect() {
            return this;
        }
        pause() {
            return this;
        }
        resume() {
            return this;
        }
    }

    class Connection {
        socket = new InMemorySocket();
        constructor(private mem: _ISchema, private queryLatency?: number) {
            bindPgServer(this.socket.peer, mem, queryLatency);
        }
    }

    return (mem: _ISchema, queryLatency?: number) => new Connection(mem, queryLatency).socket;
});


export function bindPgServer(this: void, peer: any, mem: _ISchema, queryLatency?: number) {
    const { CommandCode, bindSocket } = doRequire('pg-server') as typeof import('pg-server');
    const byCode = Object.fromEntries(
        Object.entries(CommandCode).map(([k, v]) => [v, k])
    );

    const log = typeof process !== 'undefined' && process.env.DEBUG_PG_SERVER === 'true'
        ? console.log.bind(console)
        : (...args: any[]) => { };
    let prepared: _IPreparedQuery | undefined;
    let preparedQuery: string | undefined;
    let bound: _IBoundQuery | undefined;
    let runningTx: _Transaction | undefined;
    const queue = new AsyncQueue();
    bindSocket(peer, ({ command: cmd }, writer) => queue.enqueue(async () => {
        function sendDescribe(prepared: _IPreparedQuery) {
            const p = prepared.describe();
            writer.parameterDescription(p.parameters.map(x => x.typeId));

            // see RowDescription() in connection.js of postgres.js
            //  => we just need typeId
            const descs = p.result.map<import('pg-server').FieldDesc>(x => ({
                name: x.name,
                tableID: 0,
                columnID: 0,
                dataTypeID: x.typeId,
                dataTypeSize: 0,
                dataTypeModifier: -1,
                format: 0,
                mode: 'text',
                // mode: textMode ? 'text' : 'binary',
            }))
            writer.rowDescription(descs);
        }

        function sendResults(bound: _IBoundQuery, qname: string) {
            const results = bound.executeAll(runningTx);
            if (runningTx && results.state) {
                runningTx = results.state;
            }
            for (const row of results.rows) {
                writer.dataRow(results.fields.map((x) => row[x.name]));
            }
            log('...complete', qname);
            writer.commandComplete(qname);
            writer.readyForQuery();
        }
        try {
            await delay(queryLatency ?? 0);
            const t = cmd.type;
            const cmdName = byCode[t];

            switch (t) {
                case CommandCode.init:
                    writer.authenticationOk();
                    writer.parameterStatus('client_encoding', 'UTF8');
                    writer.parameterStatus('DateStyle', 'ISO, MDY');
                    writer.parameterStatus('integer_datetimes', 'on');
                    writer.parameterStatus('server_encoding', 'UTF8');
                    writer.parameterStatus('server_version', '12.5');
                    writer.parameterStatus('TimeZone', 'UTC');

                    return writer.readyForQuery();

                case CommandCode.parse:
                    try {
                        prepared = mem.prepare(cmd.query);
                        if (!prepared) {
                            return writer.emptyQuery();
                        }
                        preparedQuery = cmd.queryName || cmd.query;
                    } catch (e: any) {
                        return writer.error(e);
                    }
                    return writer.parseComplete();
                case CommandCode.describe: {
                    if (!prepared) {
                        return writer.error("no prepared query");
                    }

                    sendDescribe(prepared);
                    return;
                }
                case CommandCode.bind: {
                    if (!prepared) {
                        return writer.error("no prepared query");
                    }
                    try {
                        bound = prepared.bind(cmd.values);
                    } catch (e: any) {
                        return writer.error(e);
                    }
                    return writer.bindComplete();
                }
                case CommandCode.execute: {
                    if (!bound || !preparedQuery) {
                        return writer.error("no bound query");
                    }
                    sendResults(bound, preparedQuery);
                    return;
                }
                case CommandCode.sync:
                    prepared = undefined;
                    preparedQuery = undefined;
                    bound = undefined;
                    // writer.readyForQuery();
                    return;
                case CommandCode.flush:
                    return;
                case CommandCode.query: {
                    if (!cmd.query) {
                        return writer.emptyQuery();
                    }

                    // handle transactions
                    const qlow = cmd.query.trim().toLowerCase();
                    switch (qlow) {
                        case 'begin':
                            runningTx = mem.db.data.fork();
                            writer.commandComplete(qlow.toUpperCase());
                            writer.readyForQuery();
                            return;
                        case 'commit':
                            if (!runningTx) {
                                return writer.error("no transaction to commit");
                            }
                            runningTx.fullCommit();
                            runningTx = undefined;
                            writer.commandComplete(qlow.toUpperCase());
                            writer.readyForQuery();
                            return;
                        case 'rollback':
                            runningTx = undefined;
                            writer.commandComplete(qlow.toUpperCase());
                            writer.readyForQuery();
                            return;
                    }

                    // simple query flow
                    const prep = mem.prepare(cmd.query);
                    sendDescribe(prep);
                    const bound = prep.bind();
                    sendResults(bound, cmd.query);
                    return;
                }
                default:
                    return writer.error(`pg-mem does not implement PG command ${cmdName}`);
            }
        } catch (e: any) {
            log("🔥 ", e);
            writer.error(e);
        }
    }));
}