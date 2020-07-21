import { IQuery, QueryError, SchemaField, DataType, IType, NotSupported } from './interfaces';
import { _IDb, _ISelection, CreateIndexColDef } from './interfaces-private';
import { watchUse } from './utils';
import { buildValue } from './predicate';
import { Types } from './datatypes';
import { JoinSelection } from './transforms/join';
import { Statement, CreateTableStatement, SelectStatement } from './parser/syntax/ast';
import { parse } from './parser/parser';



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



        // see #todo.md
        query = query.replace(/START TRANSACTION/g, '');
        query = query.replace(/COMMIT/g, '');
        query = query.replace(/ROLLBACK/g, '');
        query = query.replace(/current_schema\(\)/g, 'current_schema');

        let parsed = parse(query);
        if (!Array.isArray(parsed)) {
            parsed = [parsed];
        }
        let last;
        for (const _p of parsed) {
            if (!_p) {
                continue;
            }
            const p = watchUse(_p);
            switch (p.type) {
                // case 'insert':
                //     last = this.executeInsert(p);
                //     break;
                // case 'update':
                //     last = this.executeUpdate(p);
                //     break;
                case 'select':
                    last = this.executeSelect(p);
                    break;
                case 'create table':
                    last = this.executeCreateTable(p);
                    break;
                case 'create index':
                    last = this.executeCreateIndex(p);
                    break;
                default:
                    throw NotSupported.never(p, 'statement type');
            }
            p.check?.();
        }
        return last;
    }
    executeCreateIndex(p: any): any {
        if (p.on_kw !== 'on') {
            throw new NotSupported(p.on_kw);
        }
        if (!p.with_before_where) { // what is this ? (always true)
            throw new NotSupported();
        }
        const indexName = p.index;
        const onTable = this.db.getTable(p.table.table);
        const columns = (p.index_columns as any[])
            .map<CreateIndexColDef>(x => {
                return {
                    value: buildValue(onTable.selection, x.column),
                    nullsLast: x.nulls === 'nulls last', // nulls are first by default
                    desc: x.order === 'desc',
                }
            });
        onTable
            .createIndex({
                columns,
                indexName,
            });
    }

    executeCreateTable(p: CreateTableStatement): any {
        // get creation parameters
        const table = p.name;
        if (this.db.getTable(table, true)) {
            throw new QueryError('Table exists: ' + table);
        }

        // perform creation
        this.db.declareTable({
            name: table,
            fields: p.columns
                .map<SchemaField>(f => {
                    let primary = false;
                    let unique = false;
                    let notNull = false;
                    switch (f.constraint?.type) {
                        case 'primary key':
                            primary = true;
                            break;
                        case 'unique':
                            unique = true;
                            notNull = f.constraint.notNull;
                            break;
                        case null:
                        case undefined:
                            break;
                        default:
                            throw NotSupported.never(f.constraint);
                    }

                    const type: IType = (() => {
                        switch (f.dataType.type) {
                            case 'TEXT':
                            case 'VARCHAR':
                                return Types.text(f.dataType.length);
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
                                throw new NotSupported('Type ' + JSON.stringify(f.dataType));
                        }
                    })();

                    return {
                        id: f.name,
                        type,
                        primary,
                        unique,
                        notNull,
                    }
                })
        });
        return null;
    }

    executeSelect(p: SelectStatement): any[] {
        const t = this.buildSelect(p);
        return [...t.enumerate()];
    }

    buildSelect(p: SelectStatement): _ISelection {
        if (p.type !== 'select') {
            throw new NotSupported(p.type);
        }
        let t: _ISelection;
        const aliases = new Set<string>();
        for (const from of p.from) {
            if (!('subject' in from) || !from.subject) {
                throw new NotSupported('no table name');
            }
            const alias = typeof from.subject === 'string'
                ? from.alias ?? from.subject
                : from.alias;
            if (!alias) {
                throw new Error('No alias provided');
            }
            if (aliases.has(alias)) {
                throw new Error(`Table name "${alias}" specified more than once`)
            }
            const newT = typeof from.subject !== 'string'
                ? this.buildSelect(from.subject)
                    .setAlias(alias)
                : this.db.getSchema(from.db).getTable(from.subject)
                    .selection
                    .setAlias(alias);
            if (!t) {
                // first table to be selected
                t = newT;
                continue;
            }


            switch (from.join?.type) {
                case 'RIGHT JOIN':
                    t = new JoinSelection(this.db, newT, t, from.join.on, false);
                    break;
                case 'INNER JOIN':
                    t = new JoinSelection(this.db, t, newT, from.join.on, true);
                    break;
                case 'LEFT JOIN':
                    t = new JoinSelection(this.db, t, newT, from.join.on, false);
                    break;
                default:
                    throw new NotSupported('Joint type not supported ' + (from.join?.type ?? '<no join specified>'));
            }
        }
        t = t.filter(p.where)
            .select(p.columns);
        return t;
    }

    executeUpdate(p: any): any[] {
        throw new Error('Method not implemented.');
    }

    executeInsert(p: any): void {
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