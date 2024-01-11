import { _IStatementExecutor, _Transaction, StatementResult, _IStatement, _ISelection, NotSupported, QueryError, asSelectable, nil, OnStatementExecuted, _ISchema } from '../interfaces-private.ts';
import { WithStatementBinding, SelectStatement, SelectFromUnion, WithStatement, ValuesStatement, SelectFromStatement, QNameMapped, Name, SelectedColumn, Expr, OrderByStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { Deletion } from './records-mutations/deletion.ts';
import { Update } from './records-mutations/update.ts';
import { Insert } from './records-mutations/insert.ts';
import { ValuesTable } from '../schema/values-table.ts';
import { ignore, suggestColumnName, notNil, modifyIfNecessary } from '../utils.ts';
import { JoinSelection } from '../transforms/join.ts';
import { cleanResults } from './clean-results.ts';
import { MutationDataSourceBase } from './records-mutations/mutation-base.ts';
import { locOf } from './exec-utils.ts';
import { buildCtx, withBindingScope } from '../parser/context.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { ArrayType } from '../datatypes/index.ts';
import { RecordType } from '../datatypes/t-record.ts';
import { FunctionCallTable } from '../schema/function-call-table.ts';




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
            const prepared = (topLevel ? buildWithable(statement) : buildSelect(checkReadonlyWithable(statement)))
                .setAlias(alias.name);
            setTempBinding(alias.name, prepared);
        }
        return buildSelect(checkReadonlyWithable(p.in));
    })
}


function buildRawSelectSubject(p: SelectFromStatement): _ISelection | nil {
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
            case null:
            case undefined:
                // cross join (equivalent to INNER JOIN ON TRUE)
                sel = new JoinSelection(sel, newT, {
                    type: 'INNER JOIN',
                    on: { type: 'boolean', value: true }
                }, true);
                break;
            default:
                throw new NotSupported('Join type not supported ' + (from.join?.type ?? '<no join specified>'));
        }
    }
    return sel;
}


function buildRawSelect(p: SelectFromStatement): _ISelection {
    const distinct = !p.distinct || p.distinct === 'all'
        ? null
        : p.distinct;

    // ignore "for update" clause (not useful in non-concurrent environements)
    ignore(p.for);

    let sel = buildRawSelectSubject(p);


    // filter & select
    sel = sel ?? buildCtx().schema.dualTable.selection;
    sel = sel.filter(p.where);

    // postgres helps users: you can use group-by & order-by on aliases.
    // ... but you cant use aliases in a computation (only in simple order by statements)
    // this hack reproduces this behaviour
    const aliases = new Map(notNil(p.columns?.filter(c => !!c.alias?.name)).map(c => [c.alias!.name, c.expr]));
    const orderBy = modifyIfNecessary(p.orderBy ?? [], o => {
        const by = o.by.type === 'ref' && !o.by.table && aliases.get(o.by.name);
        return by ? { ...o, by } : null;
    });


    if (p.groupBy) {
        const groupBy = modifyIfNecessary(p.groupBy ?? [], o => {
            const group = o.type === 'ref' && !o.table && !sel?.getColumn(o.name, true) && aliases.get(o.name);
            return group || null;
        });
        sel = sel.groupBy(groupBy);
    }

    // order selection
    sel = sel.orderBy(orderBy);

    // when not grouping by, distinct is handled before
    // selection => can distinct on non selected values
    if (!p.groupBy && Array.isArray(p.distinct)) {
        sel = sel.distinct(p.distinct);
    }

    // select columns
    sel = sel.select(p.columns!);


    // when grouping by, distinct is handled after selection
    //  => can distinct on key, or selected
    if (p.groupBy && Array.isArray(p.distinct)) {
        sel = sel.distinct(p.distinct);
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
        let unnamedFields = 0;
        const nextDefaultFieldName = () => {
            const unnamedField = `column${unnamedFields || ''}`;
            unnamedFields += 1;
            return unnamedField;
        }
        return {
            result: {
                rows,
                rowCount: t.getTransient(MutationDataSourceBase.affectedRows) ?? rows.length,
                command: this.p.type.toUpperCase(),
                fields: this.selection.columns.map(
                    c => ({
                        name: c.id ?? nextDefaultFieldName(),
                        type: c.type.primary,
                        [TYPE_SYMBOL]: c.type,
                    })
                ),
                location: locOf(this.p),
            },
            state: t,
        };
    }
}

export const TYPE_SYMBOL = Symbol('type');


function checkReadonlyWithable(st: WithStatementBinding) {
    switch (st.type) {
        case 'delete':
        case 'insert':
        case 'update':
            throw new NotSupported(`"WITH" nested statement with query type '${st.type}'`);
    }
    return st;
}
