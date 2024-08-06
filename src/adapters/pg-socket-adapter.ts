import { IBoundQuery, IPreparedQuery, ISchema } from '../interfaces';
import { AsyncQueue, delay, doRequire, lazySync, nullIsh } from '../utils';

// const log = console.log.bind(console);
const log = (...args: any[]) => { };

// https://www.postgresql.org/docs/current/protocol-flow.html

export const socketAdapter = lazySync(() => {
    const { CommandCode, bindSocket } = doRequire('pg-server') as typeof import('pg-server');
    const EventEmitter = doRequire('events') as typeof import('events');

    const byCode = Object.fromEntries(
        Object.entries(CommandCode).map(([k, v]) => [v, k])
    );

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
            if (this.isTop) {
                console.log("🛜 ", data);
            }
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
        constructor(private mem: ISchema, private queryLatency?: number) {
            let prepared: IPreparedQuery | undefined;
            let preparedQuery: string | undefined;
            let bound: IBoundQuery | undefined;
            const queue = new AsyncQueue();
            bindSocket(this.socket.peer as any, ({ command: cmd }, writer) => queue.enqueue(async () => {
                await delay(this.queryLatency ?? 0);
                const t = cmd.type;
                delete (cmd as any).type;
                const cmdName = byCode[t];
                log("👉 ", cmdName, JSON.stringify(cmd));

                switch (t) {
                    case CommandCode.init:
                        return writer.readyForQuery();

                    case CommandCode.parse:
                        try {
                            prepared = mem.prepare(cmd.query);
                            if (!prepared) {
                                return writer.emptyQuery();
                            }
                            preparedQuery = cmd.query;
                        } catch (e: any) {
                            return writer.error(e);
                        }
                        return writer.parseComplete();
                    case CommandCode.describe: {
                        if (!prepared) {
                            return writer.error("no prepared query");
                        }

                        const p = prepared.describe();

                        console.log('DESC => ', p.parameters.map(x => x.typeId));
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
                        await delay(100);
                        console.log('DESC => ', descs.map(x => x.name));
                        writer.rowDescription(descs);
                        return;
                    }
                    case CommandCode.bind: {
                        if (!prepared) {
                            return writer.error("no prepared query");
                        }
                        try {
                            bound = prepared.bind(...cmd.values);
                        } catch (e: any) {
                            return writer.error(e);
                        }
                        return writer.bindComplete();
                    }
                    case CommandCode.execute: {
                        if (!bound || !preparedQuery) {
                            return writer.error("no bound query");
                        }
                        const results = bound.executeAll();
                        for (const row of results.rows) {
                            writer.dataRow(results.fields.map((x) => row[x.name]));
                        }
                        return writer.commandComplete(preparedQuery);
                    }
                    case CommandCode.sync:
                        prepared = undefined;
                        preparedQuery = undefined;
                        bound = undefined;
                        return writer.readyForQuery();
                    case CommandCode.flush:
                        return;
                    default:
                        return writer.error(`pg-mem does not implement PG command ${cmdName}`);
                }
            }));
        }
    }

    return (mem: ISchema, queryLatency?: number) => new Connection(mem, queryLatency).socket;
});
