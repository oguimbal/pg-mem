import { IBoundQuery, IPreparedQuery, QueryResult } from '../interfaces';

export class InterceptedPreparedQuery implements IPreparedQuery, IBoundQuery {
    constructor(private command: string, private result: any[]) {
    }

    bind(...args: any[]): IBoundQuery {
        return this;
    }

    *iterate(): IterableIterator<QueryResult> {
        yield this.executeAll();
    }

    executeAll(): QueryResult {
        return {
            command: this.command,
            fields: [],
            location: { start: 0, end: this.command.length },
            rowCount: 0,
            rows: this.result,
        };
    }




}