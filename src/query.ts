import { IQuery, QueryError, SchemaField, DataType, IType, NotSupported } from './interfaces';
import { _IDb, _ISelection, CreateIndexColDef } from './interfaces-private';
import { watchUse } from './utils';
import { buildValue } from './predicate';
import { Types, fromNative } from './datatypes';
import { JoinSelection } from './transforms/join';
import { Statement, CreateTableStatement, SelectStatement, InsertStatement, CreateIndexStatement } from './parser/syntax/ast';
import { parse } from './parser/parser';
import { MemoryTable } from './table';



export class Query implements IQuery {
    private dualTable = new MemoryTable(this.db, { fields: [], name: null })
        .insert({});

    constructor(private db: _IDb) {
    }

    none(query: string): void {
        this._query(query);
    }

    many(query: string): any[] {
        return this._query(query);
    }

    private _query(query: string): any[] {
        console.log(query);

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
                case 'start transaction':
                case 'commit':
                    // ignore those
                    continue;
                case 'rollback':
                    throw new QueryError('Transaction rollback not supported !');
                case 'insert':
                    last = this.executeInsert(p);
                    break;
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
    executeCreateIndex(p: CreateIndexStatement): any {
        const indexName = p.indexName;
        const onTable = this.db.getTable(p.table);
        const columns = p.expressions
            .map<CreateIndexColDef>(x => {
                return {
                    value: buildValue(onTable.selection, x.expression),
                    nullsLast: x.nulls === 'last', // nulls are first by default
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

                    return {
                        id: f.name,
                        type: fromNative(f.dataType),
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
        for (const from of p.from ?? []) {
            const alias = from.type === 'table'
                ? from.alias ?? from.table
                : from.alias;
            if (!alias) {
                throw new Error('No alias provided');
            }
            if (aliases.has(alias)) {
                throw new Error(`Table name "${alias}" specified more than once`)
            }
            // find what to select
            let newT = from.type === 'statement'
                ? this.buildSelect(from.statement)
                : this.db.getSchema(from.db).getTable(from.table)
                    .selection;

            // set its alias
            newT = newT.setAlias(alias);

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
        t = t ?? this.dualTable.selection;
        t = t.filter(p.where)
            .select(p.columns);
        return t;
    }

    executeUpdate(p: any): any[] {
        throw new Error('Method not implemented.');
    }

    executeInsert(p: InsertStatement): void {
        if (p.type !== 'insert') {
            throw new NotSupported();
        }

        // get table to insert into
        const t = this.db
            .getSchema(p.into.db)
            .getTable(p.into.table);

        // get columns to insert into
        const columns: string[] = p.columns ?? t.selection.columns.map(x => x.id);

        // get values to insert
        if (p.values) {
            const values = p.values;

            for (const val of values) {
                if (val.length !== columns.length) {
                    throw new QueryError('Insert columns / values count mismatch');
                }
                const toInsert = {};
                for (let i = 0; i < val.length; i++) {
                    const notConv = buildValue(null, val[i]);
                    const col = t.selection.getColumn(columns[i]);
                    const converted = notConv.convert(col.type);
                    if (!converted.isConstant) {
                        throw new QueryError('Cannot insert non constant expression');
                    }
                    toInsert[columns[i]] = converted.get(null);
                }
                t.insert(toInsert);
            }
        } else if (p.select) {
            const selection = this.executeSelect(p.select);
            throw new Error('todo: array-mode iteration');
        } else {
            throw new QueryError('Nothing to insert');
        }
    }
}