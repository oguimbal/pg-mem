import { ISchema } from '../interfaces';
import { delay, doRequire, lazySync } from '../utils';
import { replaceQueryArgs$ } from './adapters';

const log = console.log.bind(console);
// const log = (...args: any[]) => { };

export const socketAdapter = lazySync(() => {
    const { CommandCode, bindSocket } = doRequire('pg-server') as typeof import('pg-server');
    const { Socket } = doRequire('net') as typeof import('net');

    const byCode = Object.fromEntries(
        Object.entries(CommandCode).map(([k, v]) => [v, k])
    );

    class InMemorySocket extends Socket {
        readonly peer: InMemorySocket;
        constructor(peer?: InMemorySocket) {
            super();
            this.peer = peer ?? new InMemorySocket(this);
        }

        write(data: any, mcb1?: any, mcb2?: any) {
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
            bindSocket(this.socket.peer, async ({ command: cmd }, writer) => {
                await delay(this.queryLatency ?? 0);
                const t = cmd.type;
                delete (cmd as any).type;
                const cmdName = byCode[t];
                log("👉 ", cmdName, JSON.stringify(cmd));

                switch (t) {
                    case CommandCode.init:
                        return writer.readyForQuery();

                    case CommandCode.parse:
                        this.parsed = cmd;
                        return writer.parseComplete();
                    case CommandCode.describe: {
                        // mem.public.query(cmd.)
                        // return writer.rowDescription(this.result?.fields ?? []);
                        return;
                    }
                    case CommandCode.bind: {
                        const joined = replaceQueryArgs$(this.parsed.query, cmd.values);
                        try {
                            this.result = this.mem.public.query(joined);
                        } catch (e) {
                            console.error("execution error", e);
                            return writer.error(e);
                        }
                        // const data = parsedByName.get(cmd.name);
                        writer.rowDescription(this.result?.fields ?? []);
                        return writer.bindComplete();
                    }
                    case CommandCode.execute: {
                        // if (cmd.rows === 0) {
                        //   // execute without result
                        //   return writer.commandComplete(this.parsed.query);
                        // }
                        for (const row of this.result?.rows ?? []) {
                            const rowFlat = this.result.fields.map((x) => row[x.name]);
                            writer.dataRow(rowFlat);
                        }
                        return writer.commandComplete(this.parsed.query);
                    }
                    case CommandCode.sync:
                        return writer.readyForQuery();
                    case CommandCode.flush:
                        return writer.readyForQuery();
                    // return writer.commandComplete("ok");
                }
                debugger;
            });
        }
    }

    return (mem: ISchema, queryLatency?: number) => new Connection(mem, queryLatency).socket;
});
