import { _IStatementExecutor, _Transaction, StatementResult, _IStatement, _ISelection, NotSupported, QueryError, asSelectable, nil, OnStatementExecuted } from '../interfaces-private';
import { WithStatementBinding, SelectStatement, SelectFromUnion, WithStatement, ValuesStatement, SelectFromStatement, QNameMapped, Name, SelectedColumn } from 'pgsql-ast-parser';
import { Deletion } from './records-mutations/deletion';
import { Update } from './records-mutations/update';
import { Insert } from './records-mutations/insert';
import { ValuesTable } from 'schema/values-table';
import { ignore, suggestColumnName } from 'utils';
import { JoinSelection } from 'transforms/join';
import { cleanResults } from './clean-results';
import { MutationDataSourceBase } from './records-mutations/mutation-base';
import { locOf } from './exec-utils';

export function buildSelect(p: SelectStatement | ValuesStatement): _ISelection {
    if (p.type === 'values') {
        const ret = new ValuesTable(this.schema, '', p.values, null, acceptDefault);
        return ret.selection;
    }
}

export class SelectExec implements _IStatementExecutor {
    readonly selection: _ISelection;
    private tempBindings = new Map<string, _ISelection | 'no returning'>();

    constructor(private statement: _IStatement, private p: WithStatementBinding) {
        this.selection = this.prepareWithable(p);
    }

    get schema() {
        return this.statement.schema;
    }




    // private executeWith(t: _Transaction, p: WithStatement): QueryResult {

    //     try {
    //         // ugly hack to ensure that the insert/select behaviour of postgres is OK
    //         // see unit test "only inserts once with statement is executed" for an example.
    //         const selTrans = p.in.type === 'select' || p.in.type === 'union' ? t.fork() : t;

    //         // declare temp bindings
    //         for (const { alias, statement } of p.bind) {
    //             const prepared = this.prepareWithable(statement);
    //             if (this.tempBindings.has(alias.name)) {
    //                 throw new QueryError(` WITH query name "${alias.name}" specified more than once`);
    //             }
    //             this.tempBindings.set(alias.name, prepared.isExecutionWithNoResult ? 'no returning' : prepared);
    //         }
    //         // execute statement
    //         return this.executeWithable(selTrans, p.in);
    //     } finally {
    //         // remove temp bindings
    //         for (const { alias } of p.bind) {
    //             this.tempBindings.delete(alias.name);
    //         }
    //     }
    // }

    private prepareWithable(p: WithStatementBinding): _ISelection {
        switch (p.type) {
            case 'select':
            case 'union':
            case 'union all':
            case 'with':
            case 'with recursive':
            case 'values':
                return this.buildSelect(p);
            case 'delete':
                return new Deletion(this.statement, p);
            case 'update':
                return new Update(this.statement, p);
            case 'insert':
                return new Insert(this.statement, p);
            default:
                throw NotSupported.never(p);
        }
    }

    buildSelect(p: SelectStatement): _ISelection {
        switch (p.type) {
            case 'union':
            case 'union all':
                return this.buildUnion(p);
            case 'with':
                return this.buildWith(p);
            case 'select':
                return this.buildRawSelect(p);
            case 'values':
                return this.buildValues(p);
            case 'with recursive':
                throw new NotSupported('recursirve with statements not implemented by pg-mem');
            default:
                throw NotSupported.never(p);
        }
    }

    buildValues(p: ValuesStatement, acceptDefault?: boolean): _ISelection {
        const ret = new ValuesTable(this.schema, '', p.values, null, acceptDefault);
        return ret.selection;
    }

    private buildUnion(p: SelectFromUnion): _ISelection {
        const left = this.buildSelect(p.left);
        const right = this.buildSelect(p.right);
        const ret = left.union(right);
        if (p.type === 'union all') {
            return ret;
        }
        return ret.distinct();
    }

    private buildWith(p: WithStatement): _ISelection {
        try {
            // declare temp bindings
            for (const { alias, statement } of p.bind) {
                const prepared = this.buildSelect(checkReadonlyWithable(statement))
                    .setAlias(alias.name);
                if (this.tempBindings.has(alias.name)) {
                    throw new QueryError(` WITH query name "${alias.name}" specified more than once`);
                }
                this.tempBindings.set(alias.name, prepared.isExecutionWithNoResult ? 'no returning' : prepared);
            }
            return this.buildSelect(checkReadonlyWithable(p.in));
        } finally {
            // remove temp bindings
            for (const { alias } of p.bind) {
                this.tempBindings.delete(alias.name);
            }
        }
    }

    private buildRawSelect(p: SelectFromStatement): _ISelection {
        const distinct = !p.distinct || p.distinct === 'all'
            ? null
            : p.distinct;

        // ignore "for update" clause (not useful in non-concurrent environements)
        ignore(p.for);

        // compute data source
        let sel: _ISelection | undefined = undefined;
        for (const from of p.from ?? []) {
            // find what to select
            let newT: _ISelection;
            switch (from.type) {
                case 'table':
                    newT = this.getSelectable(from.name);
                    break;
                case 'statement':
                    newT = this.mapColumns(from.alias
                        , this.buildSelect(from.statement)
                        , from.columnNames
                        , true)
                        .setAlias(from.alias);
                    break;
                case 'call':
                    const fnName = from.alias?.name ?? from.function?.name;
                    newT = new ValuesTable(this.schema, fnName, [[from]], [fnName])
                        .setAlias(from.alias?.name ?? suggestColumnName(from) ?? '');
                    break;
                default:
                    throw NotSupported.never(from);
            }

            // if (!!newT.name && aliases.has(newT.name)) {
            //     throw new Error(`Alias name "${newT.name}" specified more than once`)
            // }

            if (!sel) {
                // first table to be selected
                sel = newT;
                continue;
            }

            switch (from.join?.type) {
                case 'INNER JOIN':
                    sel = new JoinSelection(this.statement, sel, newT, from.join!, true);
                    break;
                case 'LEFT JOIN':
                    sel = new JoinSelection(this.statement, sel, newT, from.join!, false);
                    break;
                case 'RIGHT JOIN':
                    sel = new JoinSelection(this.statement, newT, sel, from.join!, false);
                    break;
                default:
                    throw new NotSupported('Joint type not supported ' + (from.join?.type ?? '<no join specified>'));
            }
        }

        // filter & select
        sel = sel ?? this.schema.dualTable.selection;
        sel = sel.filter(p.where);

        if (p.groupBy) {
            sel = sel.groupBy(p.groupBy, p.columns!);
            sel = sel.orderBy(p.orderBy);

            // when grouping by, distinct is handled after selection
            //  => can distinct on key, or selected
            if (Array.isArray(p.distinct)) {
                sel = sel.distinct(p.distinct);
            }
        } else {
            sel = sel.orderBy(p.orderBy);

            // when not grouping by, distinct is handled before
            // selection => can distinct on non selected values
            if (Array.isArray(p.distinct)) {
                sel = sel.distinct(p.distinct);
            }

            sel = sel.select(p.columns!);
        }

        // handle 'distinct' on result set
        if (distinct === 'distinct') {
            sel = sel.distinct();
        }

        if (p.limit) {
            sel = sel.limit(p.limit);
        }
        return sel;
    }

    private getSelectable(name: QNameMapped): _ISelection<any> {
        const temp = !name.schema
            && this.tempBindings.get(name.name);
        if (temp === 'no returning') {
            throw new QueryError(`WITH query "${name.name}" does not have a RETURNING clause`);
        }
        let ret = temp || asSelectable(this.schema.getObject(name)).selection;

        ret = this.mapColumns(name.name, ret, name.columnNames, false);

        if (name.alias) {
            ret = ret.setAlias(name.alias);
        }
        return ret;
    }

    private mapColumns(tableName: string, sel: _ISelection, columnNames: Name[] | nil, appendNonMapped: boolean) {
        if (!columnNames?.length) {
            return sel;
        }
        if (columnNames.length > sel.columns.length) {
            throw new QueryError(`table "${tableName}" has ${sel.columns.length} columns available but ${columnNames.length} columns specified`, '42P10')
        }

        const mapped = new Set<string>(columnNames.map(x => x.name));
        const cols = sel.columns.map<SelectedColumn>((col, i) => ({
            expr: {
                type: 'ref',
                name: col.id!,
            },
            // when realiasing table columns, columns which have not been mapped
            //  must not be removed
            // see ut "can map column names"
            alias: columnNames[i]
                ?? {
                name: mapped.has(sel.columns[i].id!)
                    ? `${sel.columns[i].id!}1`
                    : sel.columns[i].id!,
            },
        }));

        return sel.select(
            cols
        )
    }



    execute(t: _Transaction): StatementResult {
        const rows = cleanResults([...this.selection.enumerate(t)]);
        return {
            result: {
                rows,
                rowCount: t.getTransient(MutationDataSourceBase.affectedRows) ?? rows.length,
                command: this.p.type.toUpperCase(),
                fields: [],
                location: locOf(this.p),
            },
            state: t,
        };
    }
}



function checkReadonlyWithable(st: WithStatementBinding) {
    switch (st.type) {
        case 'delete':
        case 'insert':
        case 'update':
            throw new NotSupported(`"WITH" nested statement with query type '${st.type}'`);
    }
    return st;
}
