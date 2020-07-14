import { Parser, Insert_Replace, Update, Select } from 'node-sql-parser';
import { IQuery, TableNotFound, QueryError, CastError } from './interfaces';
import { _IDb } from './interfaces-private';
import { NotSupported, trimNullish } from './utils';
import { buildValue } from './predicate';

export class Query implements IQuery {

    constructor(private db: _IDb) {
    }

    async none(query: string): Promise<void> {
        await this._query(query);
    }

    many(query: string): Promise<any[]> {
        return this._query(query);
    }

    private async _query(query: string): Promise<any[]> {
        const parser = new Parser();
        let parsed = parser.astify(query, {
            database: 'PostgresQL',
        });
        if (!Array.isArray(parsed)) {
            parsed = [parsed];
        }
        let last;
        for (const p of parsed) {
            switch (p.type) {
                case 'insert':
                    last = this.executeInsert(p);
                    break;
                case 'update':
                    last = this.executeUpdate(p);
                    break;
                case 'select':
                    last = this.executeSelect(p);
                    break;
                default:
                    throw new NotSupported();
            }
        }
        return last;
    }

    executeSelect(p: Select): any[] {
        if (p.type !== 'select') {
            throw new NotSupported();
        }
        delete p.type;
        p = trimNullish(p);
        if (p.from?.length !== 1) {
            throw new NotSupported();
        }
        const [from] = p.from;
        delete p.from;
        if (!('table' in from) || !from.table) {
            throw new NotSupported();
        }
        const t = this.db.getTable(from.table)
            .selection
            .filter(p.where)
            .select(p.columns);
        delete p.where;
        delete p.columns;
        if (Object.keys(p).length) {
            throw new NotSupported();
        }
        return [...t.enumerate()];
    }

    executeUpdate(p: Update): any[] {
        throw new Error('Method not implemented.');
    }

    executeInsert(p: Insert_Replace): void {
        if (p.type !== 'insert') {
            throw new NotSupported();
        }
        delete p.type;
        p = trimNullish(p);
        if (p.table?.length !== 1) {
            throw new NotSupported();
        }

        // get table to insert into
        let [into] = p.table;
        delete p.table;
        if (!('table' in into) || !into.table) {
            throw new NotSupported();
        }
        const intoTable = into.table;
        delete into.table;
        if (Object.keys(trimNullish(into)).length > 0) {
            throw new NotSupported();
        }
        const t = this.db.getTable(intoTable);
        if (!t) {
            throw new TableNotFound(intoTable);
        }

        // get columns to insert into
        const columns: string[] = p.columns ?? t.selection.columns.map(x => x.id);
        delete p.columns;

        // get values to insert
        const values = p.values;
        delete p.values;

        for (const val of values) {
            if (val.type !== 'expr_list') {
                throw new NotSupported('insert value type ' + val.type);
            }
            if (val.value.length !== columns.length) {
                throw new QueryError('Insert columns / values count mismatch');
            }
            const toInsert = {};
            for (let i = 0; i < val.value.length; i++) {
                const notConv = buildValue(null, val.value[i]);
                const col = t.selection.getColumn(columns[i]);
                if (!notConv.canConvert(col.type)) {
                    throw new CastError(notConv.type, col.type);
                }
                const converted = notConv.convert(col.type);
                toInsert[columns[i]] = converted.get(null);
            }
            t.insert(toInsert);
        }

        if (Object.keys(p).length) {
            throw new NotSupported();
        }
    }
}