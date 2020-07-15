import { Parser, Insert_Replace, Update, Select } from 'node-sql-parser';
import { IQuery, TableNotFound, QueryError, CastError, SchemaField, DataType, IType } from './interfaces';
import { _IDb, AST2, CreateTable } from './interfaces-private';
import { NotSupported, trimNullish, watchUse } from './utils';
import { buildValue } from './predicate';
import { Types } from './datatypes';



export class Query implements IQuery {

    constructor(private db: _IDb) {
    }

    none(query: string): void {
        this._query(query);
    }

    many(query: string): any[] {
        return this._query(query);
    }

    private _query(query: string): any[] {
        const parser = new Parser();
        let parsed = parser.astify(query, {
            database: 'PostgresQL',
        }) as AST2 | AST2[];
        if (!Array.isArray(parsed)) {
            parsed = [parsed];
        }
        let last;
        for (const _p of parsed) {
            const p = watchUse(_p);
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
                case 'create':
                    last = this.executeCreate(p);
                    break;
                default:
                    throw new NotSupported();
            }
            p.check?.();
        }
        return last;
    }

    executeCreate(p: CreateTable): any {
        switch (p.keyword) {
            case 'table':
                // get creation parameters
                const [{ table }] = p.table;
                const def = p.create_definitions;

                // perform creation
                this.db.declareTable({
                    name: table,
                    fields: def.filter(f => f.resource === 'column')
                        .map<SchemaField>(f => {
                            if (f.column.type !== 'column_ref') {
                                throw new NotSupported(f.column.type);
                            }
                            let primary = false;
                            switch (f.unique_or_primary) {
                                case 'primary key':
                                    primary = true;
                                    break;
                                case null:
                                case undefined:
                                    break;
                                default:
                                    throw new NotSupported(f.unique_or_primary);
                            }

                            const type: IType = (() => {
                                switch (f.definition.dataType) {
                                    case 'TEXT':
                                    case 'VARCHAR':
                                        return Types.text(f.definition.length);
                                    case 'INT':
                                    case 'INTEGER':
                                        return Types.int;
                                    case 'DECIMAL':
                                    case 'FLOAT':
                                        return Types.float;
                                    case 'TIMESTAMP':
                                        return Types.timestamp;
                                    case 'DATE':
                                        return Types.date;
                                    case 'JSON':
                                        return Types.json;
                                    case 'JSONB':
                                        return Types.jsonb;
                                    default:
                                        throw new NotSupported('Type ' + JSON.stringify(f.definition.dataType));
                                }
                            })();

                            if (f.definition.suffix?.length) {
                                throw new NotSupported('column suffix');
                            }

                            return {
                                id: f.column.column,
                                type,
                                primary,
                            }
                        })
                });
                return null;
            default:
                throw new NotSupported('create ' + p.keyword);
        }
        throw new Error('Method not implemented.');
    }

    executeSelect(p: Select): any[] {
        if (p.type !== 'select') {
            throw new NotSupported();
        }
        if (p.from?.length !== 1) {
            throw new NotSupported();
        }
        const [from] = p.from;
        if (!('table' in from) || !from.table) {
            throw new NotSupported();
        }
        const t = this.db.getTable(from.table)
            .selection
            .filter(p.where)
            .select(p.columns);
        return [...t.enumerate()];
    }

    executeUpdate(p: Update): any[] {
        throw new Error('Method not implemented.');
    }

    executeInsert(p: Insert_Replace): void {
        if (p.type !== 'insert') {
            throw new NotSupported();
        }
        if (p.table?.length !== 1) {
            throw new NotSupported();
        }

        // get table to insert into
        let [into] = p.table;
        if (!('table' in into) || !into.table) {
            throw new NotSupported();
        }
        const intoTable = into.table;
        const t = this.db.getTable(intoTable);
        if (!t) {
            throw new TableNotFound(intoTable);
        }

        // get columns to insert into
        const columns: string[] = p.columns ?? t.selection.columns.map(x => x.id);

        // get values to insert
        const values = p.values;

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
                const converted = notConv.convert(col.type);
                toInsert[columns[i]] = converted.get(null);
            }
            t.insert(toInsert);
        }
    }
}