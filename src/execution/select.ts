import { _IStatementExecutor, _Transaction, StatementResult, _IStatement, _ISelection, NotSupported, QueryError, asSelectable, nil, OnStatementExecuted, _ISchema } from '../interfaces-private';
import { WithStatementBinding, SelectStatement, SelectFromUnion, WithStatement, ValuesStatement, SelectFromStatement, QNameMapped, Name, SelectedColumn } from 'pgsql-ast-parser';
import { Deletion } from './records-mutations/deletion';
import { Update } from './records-mutations/update';
import { Insert } from './records-mutations/insert';
import { ValuesTable } from '../schema/values-table';
import { ignore, suggestColumnName } from '../utils';
import { JoinSelection } from '../transforms/join';
import { cleanResults } from './clean-results';
import { MutationDataSourceBase } from './records-mutations/mutation-base';
import { locOf } from './exec-utils';
import { buildCtx, withBindingScope } from '../parser/context';
import { buildValue } from '../parser/expression-builder';
import { DataType } from '../interfaces';
import { ArrayType } from '../datatypes';
import { RecordType } from '../datatypes/t-record';
import { FunctionCallTable } from '../schema/function-call-table';




export function buildValues(p: ValuesStatement, acceptDefault?: boolean): _ISelection {
    const ret = new ValuesTable('', p.values, null, acceptDefault);
    return ret.selection;
}


function buildWithable(p: WithStatementBinding): _ISelection {
    switch (p.type) {
        case 'select':
        case 'union':
        case 'union all':
        case 'with':
        case 'with recursive':
        case 'values':
            return buildSelect(p);
        case 'delete':
            return new Deletion(p);
        case 'update':
            return new Update(p);
        case 'insert':
            return new Insert(p);
        default:
            throw NotSupported.never(p);
    }
}

export function buildSelect(p: SelectStatement): _ISelection {
    switch (p.type) {
        case 'union':
        case 'union all':
            return buildUnion(p);
        case 'with':
            return buildWith(p, false);
        case 'select':
            return buildRawSelect(p);
        case 'values':
            return buildValues(p);
        case 'with recursive':
            throw new NotSupported('recursirve with statements not implemented by pg-mem');
        default:
            throw NotSupported.never(p);
    }
}


function buildUnion(p: SelectFromUnion): _ISelection {
    const left = buildSelect(p.left);
    const right = buildSelect(p.right);
    const ret = left.union(right);
    if (p.type === 'union all') {
        return ret;
    }
    return ret.distinct();
}

export function buildWith(p: WithStatement, topLevel: boolean): _ISelection {
    return withBindingScope(() => {
        const { setTempBinding } = buildCtx();
        // declare temp bindings
        for (const { alias, statement } of p.bind) {
            const prepared = topLevel ? buildWithable(statement) : buildSelect(checkReadonlyWithable(statement))
                .setAlias(alias.name);
            setTempBinding(alias.name, prepared);
        }
        return buildSelect(checkReadonlyWithable(p.in));
    })
}



function buildRawSelect(p: SelectFromStatement): _ISelection {
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
                newT = getSelectable(from.name);
                break;
            case 'statement':
                newT = mapColumns(from.alias
                    , buildSelect(from.statement)
                    , from.columnNames
                    , true)
                    .setAlias(from.alias);
                break;
            case 'call':
                const fnName = from.alias?.name ?? from.function?.name;
                const fromValue = buildValue(from);
                if (ArrayType.matches(fromValue.type) && RecordType.matches(fromValue.type.of)) {
                    // if the function returns an array of records (= "a table"), then lets use it as a table
                    const cols = fromValue.type.of.columns;
                    newT = new FunctionCallTable(cols, fromValue);
                } else {
                    // if the function returns a single value, then lets transform this into a table
                    // nb: the function call will be re-built in here, but its OK (coz' of build cache)
                    newT = new ValuesTable(fnName, [[from]], [fnName])
                        .setAlias(from.alias?.name ?? suggestColumnName(from) ?? '');
                }
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
                sel = new JoinSelection(sel, newT, from.join!, true);
                break;
            case 'LEFT JOIN':
                sel = new JoinSelection(sel, newT, from.join!, false);
                break;
            case 'RIGHT JOIN':
                sel = new JoinSelection(newT, sel, from.join!, false);
                break;
            default:
                throw new NotSupported('Joint type not supported ' + (from.join?.type ?? '<no join specified>'));
        }
    }

    // filter & select
    sel = sel ?? buildCtx().schema.dualTable.selection;
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

function getSelectable(name: QNameMapped): _ISelection {
    const { schema, getTempBinding } = buildCtx();
    const temp = !name.schema
        && getTempBinding(name.name);

    let ret = temp || asSelectable(schema.getObject(name)).selection;
    ret = mapColumns(name.name, ret, name.columnNames, false);

    if (name.alias) {
        ret = ret.setAlias(name.alias);
    }
    return ret;
}

function mapColumns(tableName: string, sel: _ISelection, columnNames: Name[] | nil, appendNonMapped: boolean) {
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


export class SelectExec implements _IStatementExecutor {
    readonly selection: _ISelection;

    constructor(private statement: _IStatement, private p: WithStatementBinding) {
        // a bit of a special case for top level withs.
        this.selection = p.type === 'with' ? buildWith(p, true) : buildWithable(p);
    }

    get schema() {
        return this.statement.schema;
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
