import { _IStatementExecutor, _Transaction, StatementResult, _IStatement, _ISelection, IValue, NotSupported, QueryError, asSelectable, asTable, nil, OnStatementExecuted, _ISchema, setId } from '../interfaces-private.ts';
import { WithStatementBinding, SelectStatement, SelectFromUnion, WithStatement, WithRecursiveStatement, ValuesStatement, SelectFromStatement, QNameMapped, Name, SelectedColumn, Expr, OrderByStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { Deletion } from './records-mutations/deletion.ts';
import { Update } from './records-mutations/update.ts';
import { Insert } from './records-mutations/insert.ts';
import { ValuesTable } from '../schema/values-table.ts';
import { ignore, suggestColumnName, notNil, modifyIfNecessary, asSingleQName } from '../utils.ts';
import { applyReadRls } from './rls-enforce.ts';
import { JoinSelection } from '../transforms/join.ts';
import { buildSetOp } from '../transforms/union.ts';
import { expandGroupingSets } from './grouping-sets.ts';
import { buildWindow, exprsHaveWindow } from '../transforms/window.ts';
import { RecursiveCte, RecursiveCteBuffer } from './recursive-cte.ts';
import { expandSrfs } from '../transforms/expand-srf.ts';
import { LateralCallTable } from '../transforms/lateral-call.ts';
import { MutationDataSourceBase } from './records-mutations/mutation-base.ts';
import { locOf } from './exec-utils.ts';
import { buildCtx, withBindingScope, withSelection } from '../parser/context.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { ArrayType, Types } from '../datatypes/index.ts';
import { Evaluator } from '../evaluator.ts';
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
        case 'intersect':
        case 'intersect all':
        case 'except':
        case 'except all':
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
        case 'intersect':
        case 'intersect all':
        case 'except':
        case 'except all':
            return buildUnion(p);
        case 'with':
            return buildWith(p, false);
        case 'select': {
            // GROUP BY ROLLUP/CUBE/GROUPING SETS expands to a UNION ALL of grouping sets
            const expanded = expandGroupingSets(p);
            return expanded ? buildSelect(expanded) : buildRawSelect(p);
        }
        case 'values':
            return buildValues(p);
        case 'with recursive':
            return buildWithRecursive(p);
        default:
            throw NotSupported.never(p);
    }
}


function buildUnion(p: SelectFromUnion): _ISelection {
    const left = buildSelect(p.left);
    const right = buildSelect(p.right);
    switch (p.type) {
        case 'union':
            return left.union(right).distinct();
        case 'union all':
            return left.union(right);
        case 'intersect':
            return buildSetOp(left, right, 'intersect', false);
        case 'intersect all':
            return buildSetOp(left, right, 'intersect', true);
        case 'except':
            return buildSetOp(left, right, 'except', false);
        case 'except all':
            return buildSetOp(left, right, 'except', true);
    }
}

function buildWithRecursive(p: WithRecursiveStatement): _ISelection {
    return withBindingScope(() => {
        const { setTempBinding } = buildCtx();
        const seed = buildSelect(p.bind.left);
        // the column list is optional: when omitted, take the names the seed produces
        const names = p.columnNames
            ? p.columnNames.map(x => x.name)
            : seed.columns.map((c, i) => c.id ?? `column${i + 1}`);
        if (p.columnNames && names.length !== seed.columns.length) {
            throw new QueryError(`table "${p.alias.name}" has ${seed.columns.length} columns available but ${names.length} columns specified`, '42P10');
        }
        const cols = names.map((name, i) => ({ name, type: seed.columns[i].type }));
        const buffer = new RecursiveCteBuffer(cols);
        const result = new RecursiveCte(cols, seed, buffer, p.bind.type === 'union');
        // the recursive term only sees the previous iteration's rows (the working buffer)
        result.setRecursive(withBindingScope(() => {
            buildCtx().setTempBinding(p.alias.name, buffer.setAlias(p.alias.name));
            return buildSelect(p.bind.right);
        }));
        // the outer statement sees the full recursive result
        setTempBinding(p.alias.name, result.setAlias(p.alias.name));
        return buildSelect(checkReadonlyWithable(p.in));
    });
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
                // built against the left FROM items: function calls referencing them
                // are implicitly lateral in postgres
                const fromValue: IValue = sel ? withSelection(sel, () => buildValue(from)) : buildValue(from);
                if ((from.lateral || !fromValue.isConstant) && sel) {
                    if (!ArrayType.matches(fromValue.type) || RecordType.matches(fromValue.type.of)) {
                        throw new NotSupported('lateral function calls returning records');
                    }
                    sel = new LateralCallTable(sel, fromValue, fnName!);
                    continue;
                }
                if (ArrayType.matches(fromValue.type) && RecordType.matches(fromValue.type.of)) {
                    // if the function returns an array of records (= "a table"), then lets use it as a table
                    const cols = fromValue.type.of.columns;
                    newT = new FunctionCallTable(cols, fromValue);
                } else if (ArrayType.matches(fromValue.type)) {
                    // set-returning function over scalars (generate_series, unnest, ...):
                    // one row per element, single column named after the function or its alias.
                    // WITH ORDINALITY appends a 1-based bigint position column.
                    const aliasCols = from.alias?.columns;
                    const withOrd = !!from.withOrdinality;
                    const valName = aliasCols?.[0]?.name ?? fnName!;
                    const ordName = aliasCols?.[1]?.name ?? 'ordinality';
                    const colDefs = withOrd
                        ? [{ name: valName, type: fromValue.type.of }, { name: ordName, type: Types.bigint }]
                        : [{ name: valName, type: fromValue.type.of }];
                    const recType = Types.record(colDefs) as RecordType;
                    const rows = new Evaluator(
                        recType.asArray()
                        , null
                        , `${fromValue.hash}_rows${withOrd ? '_ord' : ''}`
                        , [fromValue]
                        , (raw, t) => (fromValue.get(raw, t) ?? [])
                            .map((v: any, i: number) => setId(
                                withOrd ? { [valName]: v, [ordName]: String(i + 1) } : { [valName]: v }
                                , `srf_${fromValue.hash}_${i}`)));
                    newT = new FunctionCallTable(recType.columns, rows);
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
            case 'FULL JOIN':
                sel = new JoinSelection(sel, newT, from.join!, false, true);
                break;
            case 'CROSS JOIN':
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

    // window functions annotate rows before the final projection reads them
    if (exprsHaveWindow(p.columns?.map(c => c.expr))) {
        sel = buildWindow(sel);
    }

    // select columns
    sel = sel.select(p.columns!);

    // set-returning functions in the select list expand the projected rows
    sel = expandSrfs(sel);


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

    let ret: _ISelection;
    if (temp) {
        ret = temp;
    } else {
        const obj = schema.getObject(name);
        ret = asSelectable(obj).selection;
        // row-level security: a table read in a FROM clause is filtered by its
        // SELECT policies (no-op when RLS is off or the role bypasses it)
        const table = asTable(obj, true);
        if (table) {
            ret = applyReadRls(table, ret, 'select');
        }
    }
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
        const rows = [...this.selection.enumerate(t)];
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
                        typeId: c.type.reg.typeId,
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
