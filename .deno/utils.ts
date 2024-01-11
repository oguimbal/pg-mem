import moment from 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts';
import { List } from 'https://deno.land/x/immutable@4.0.0-rc.12-deno.1/mod.ts';
import { IValue, NotSupported, RegClass, _IRelation, _ISchema, _ISelection, _ITable, _IType, _Transaction } from './interfaces-private.ts';
import { BinaryOperator, DataTypeDef, Expr, ExprRef, ExprValueKeyword, Interval, nil, parse, QName, SelectedColumn } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ColumnNotFound, ISubscription, IType, QueryError, typeDefToStr } from './interfaces.ts';
import { bufClone, bufCompare, isBuf } from './misc/buffer-deno.ts';

export interface Ctor<T> extends Function {
    new(...params: any[]): T; prototype: T;
}

export type Optional<T> = { [key in keyof T]?: T[key] };
export type SRecord<T> = Record<string, T>;


export function trimNullish<T>(value: T, depth = 5): T {
    if (depth < 0)
        return value;
    if (value instanceof Array) {
        value.forEach(x => trimNullish(x, depth - 1))
    }
    if (typeof value !== 'object' || value instanceof Date || moment.isMoment(value) || moment.isDuration(value))
        return value;

    if (!value) {
        return value;
    }

    for (const k of Object.keys(value)) {
        const val = (value as any)[k];
        if (nullIsh(val))
            delete (value as any)[k];
        else
            trimNullish(val, depth - 1);
    }
    return value;
}


export function watchUse<T>(rootValue: T): { checked: T; check?: () => string | null; } {
    if (!rootValue || typeof globalThis !== 'undefined' && (globalThis as any)?.process?.env?.['NOCHECKFULLQUERYUSAGE'] === 'true') {
        return { checked: rootValue };
    }
    if (typeof rootValue !== 'object') {
        throw new NotSupported();
    }
    if (Array.isArray(rootValue)) {
        throw new NotSupported();
    }
    const toUse = new Map<string, any>();
    function recurse(value: any, stack: List<string> = List()): any {
        if (!value || typeof value !== 'object') {
            return value;
        }
        if (Array.isArray(value)) {
            return value
                .map((x, i) => recurse(x, stack.push(`[${i}]`)));
        }
        // watch object
        const ret: any = {};
        for (const [k, _v] of Object.entries(value)) {
            if (k[0] === '_') { // ignore properties starting with '_'
                ret[k] = _v;
                continue;
            }
            const nstack = stack.push('.' + k);
            let v = recurse(_v, nstack);
            const nstackKey = nstack.join('');
            toUse.set(nstackKey, _v);
            Object.defineProperty(ret, k, {
                get() {
                    toUse.delete(nstackKey);
                    return v;
                },
                enumerable: true,
            });
        }
        return ret;
    }

    const final = recurse(rootValue);

    const check = function () {
        if (toUse.size) {
            return `The query you ran generated an AST which parts have not been read by the query planner. \
This means that those parts could be ignored:

    ⇨ ` + [...toUse.entries()]
                    .map(([k, v]) => k + ' (' + JSON.stringify(v) + ')')
                    .join('\n    ⇨ ');
        }
        return null;
    }
    return { checked: final, check };
}



export function deepEqual<T>(a: T, b: T, strict?: boolean, depth = 10, numberDelta = 0.0001) {
    return deepCompare(a, b, strict, depth, numberDelta) === 0;
}

export function deepCompare<T>(a: T, b: T, strict?: boolean, depth = 10, numberDelta = 0.0001): number {
    if (depth < 0) {
        throw new NotSupported('Comparing too deep entities');
    }

    if (a === b) {
        return 0;
    }
    if (!strict) {
        // should not use '==' because it could call .toString() on objects when compared to strings.
        // ... which is not ok. Especially when working with translatable objects, which .toString() returns a transaltion (a string, thus)
        if (!a && !b) {
            return 0;
        }
    }

    if (Array.isArray(a)) {
        if (!Array.isArray(b)) {
            return -1; // [] < {}
        }
        if (a.length !== b.length) {
            return a.length > b.length ? 1 : -1;
        }
        for (let i = 0; i < a.length; i++) {
            const inner = deepCompare(a[i], b[i], strict, depth - 1, numberDelta);
            if (inner)
                return inner;
        }
        return 0;
    }

    if (Array.isArray(b)) {
        return 1;
    }

    if (isBuf(a) || isBuf(b)) {
        if (!isBuf(a)) {
            return 1;
        }
        if (!isBuf(b)) {
            return -1;
        }
        return bufCompare(a, b);
    }

    // handle dates
    if (a instanceof Date || b instanceof Date || moment.isMoment(a) || moment.isMoment(b)) {
        const am = moment(a);
        const bm = moment(b);
        if (am.isValid() !== bm.isValid()) {
            return am.isValid()
                ? -1
                : 1;
        }
        const diff = am.diff(bm, 'seconds');
        if (Math.abs(diff) < 0.001) {
            return 0;
        }
        return diff > 0 ? 1 : -1;
    }

    // handle durations
    if (moment.isDuration(a) || moment.isDuration(b)) {
        const da = moment.duration(a);
        const db = moment.duration(b);
        if (da.isValid() !== db.isValid()) {
            return da.isValid()
                ? -1
                : 1;
        }
        const diff = da.asMilliseconds() - db.asMilliseconds();
        if (Math.abs(diff) < 1) {
            return 0;
        }
        return diff > 0 ? 1 : -1;
    }

    const fa = Number.isFinite(<any>a);
    const fb = Number.isFinite(<any>b);
    if (fa && fb) {
        if (Math.abs(<any>a - <any>b) <= numberDelta) {
            return 0;
        }
        return a > b ? 1 : -1;
    } else if (fa && b) {
        return -1;
    } else if (fb && a) {
        return 1;
    }

    // === handle plain objects
    if (typeof a !== 'object') {
        return 1; // objects are at the end
    }
    if (typeof b !== 'object') {
        return -1; // objects are at the end
    }

    if (!a || !b) {
        return 0; // nulls
    }

    const ak = Object.keys(a);
    const bk = Object.keys(b);
    if (strict && ak.length !== bk.length) {
        // longer objects at the end
        return ak.length > bk.length ? 1 : -1;
    }
    const set: Iterable<string> = strict
        ? Object.keys(a)
        : new Set([...Object.keys(a), ...Object.keys(b)]);
    for (const k of set) {
        const inner = deepCompare((a as any)[k], (b as any)[k], strict, depth - 1, numberDelta);
        if (inner) {
            return inner;
        }
    }
    return 0;
}


type Json = { [key: string]: Json } | Json[] | string | number | null;
export function queryJson(a: Json, b: Json) {
    if (!a || !b) {
        return (a ?? null) === (b ?? null);
    }
    if (a === b) {
        return true;
    }

    if (typeof a === 'string' || typeof b === 'string') {
        return false;
    }

    if (typeof a === 'number' || typeof b === 'number') {
        return false;
    }

    if (Array.isArray(a)) {
        // expecting array
        if (!Array.isArray(b)) {
            return false;
        }
        // => must match all those criteria
        const toMatch = [...a];
        for (const be of b) {
            for (let i = 0; i < toMatch.length; i++) {
                if (queryJson(toMatch[i], be)) {
                    // matched this criteria
                    toMatch.splice(i, 1);
                    break;
                }
            }
            if (!toMatch.length) {
                break;
            }
        }
        return !toMatch.length;
    }

    if (Array.isArray(b)) {
        return false;
    }

    if ((typeof a === 'object') !== (typeof b === 'object')) {
        return false;
    }
    const akeys = Object.keys(a);
    const bkeys = Object.keys(b);
    if (akeys.length > bkeys.length) {
        return false;
    }
    for (const ak of akeys) {
        if (!(ak in (b as any))) {
            return false;
        }
        if (!queryJson(a[ak], b[ak])) {
            return false;
        }
    }
    return true;
}

export function buildLikeMatcher(likeCondition: string, caseSensitive = true) {
    // Escape regex characters from likeCondition
    likeCondition = likeCondition.replace(/[-[\]{}()*+?.,\\^$|#\s]/g, '\\$&');
    let likeRegexString = likeCondition.replace(/\%/g, ".*").replace(/_/g, '.');
    likeRegexString = "^" + likeRegexString + "$";
    const reg = new RegExp(likeRegexString, caseSensitive ? '' : 'i');

    return (stringToMatch: string | number) => {
        if (nullIsh(stringToMatch)) {
            return null;
        }
        if (typeof stringToMatch != "string") {
            stringToMatch = stringToMatch.toString();
        }
        return reg.test(stringToMatch);
    }
}

export function nullIsh(v: any): v is nil {
    return v === null || v === undefined;
}

export function hasNullish(...vals: any[]): boolean {
    return vals.some(v => nullIsh(v));
}

export function sum(v: number[]): number {
    return v.reduce((sum, el) => sum + el, 0);
}

export function deepCloneSimple<T>(v: T): T {
    if (!v || typeof v !== 'object' || v instanceof Date) {
        return v;
    }
    if (Array.isArray(v)) {
        return (v as any[]).map(x => deepCloneSimple(x)) as any;
    }
    if (isBuf(v)) {
        return bufClone(v) as any;
    }

    const ret: any = {};
    for (const k of Object.keys(v)) {
        ret[k] = deepCloneSimple((v as any)[k]);
    }
    for (const k of Object.getOwnPropertySymbols(v)) {
        ret[k] = (v as any)[k]; // no need to deep clone that
    }
    return ret;
}


export function isSelectAllArgList(select: Expr[]): boolean {
    const [first] = select;
    return select.length === 1
        && first.type === 'ref'
        && first.name === '*'
        && !first.table;
}


export function ignore(...val: any[]): void {
    for (const v of val) {
        if (!v) {
            continue;
        }
        if (Array.isArray(v)) {
            ignore(...v);
            continue;
        }
        if (typeof v !== 'object') {
            continue;
        }
        ignore(...Object.values(v));
    }
}

export function combineSubs(...vals: ISubscription[]): ISubscription {
    return {
        unsubscribe: () => {
            vals.forEach(u => u?.unsubscribe());
        },
    };
}


export interface ExecCtx {
    readonly schema: _ISchema;
    readonly transaction: _Transaction;
    readonly parametersValues?: any[];
}
const curCtx: ExecCtx[] = [];
export function executionCtx(): ExecCtx {
    if (!curCtx.length) {
        throw new Error('No execution context available');
    }
    return curCtx[curCtx.length - 1];
}
export function hasExecutionCtx(): boolean {
    return curCtx.length > 0;
}

export function pushExecutionCtx<T>(ctx: ExecCtx, act: () => T): T {
    try {
        curCtx.push(ctx)
        return act();
    } finally {
        curCtx.pop();
    }
}

export function indexHash(this: void, vals: (IValue | string)[]) {
    return vals.map(x => typeof x === 'string' ? x : x.hash).sort().join('|');
}

export function randomString(length = 8, chars = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'): string {
    var result = '';
    for (var i = length; i > 0; --i) result += chars[Math.floor(Math.random() * chars.length)];
    return result;
}


export function schemaOf(t: DataTypeDef): string | nil {
    if (t.kind === 'array') {
        return schemaOf(t.arrayOf);
    }
    return t.schema;
}


export function isType(t: any): t is (_IType | IType) {
    return !!t?.[isType.TAG];
}
isType.TAG = Symbol();


export function suggestColumnName(expr: Expr | nil): string | null {
    if (!expr) {
        return null;
    }
    // suggest a column result name
    switch (expr.type) {
        case 'call':
            return expr.function.name;
        case 'ref':
            return expr.name;
        case 'keyword':
            return expr.keyword;
        case 'cast':
            return typeDefToStr(expr.to);
    }
    return null;
}

export function findTemplate<T>(this: void, selection: _ISelection, t: _Transaction, template?: T, columns?: (keyof T)[]): Iterable<T> {
    // === Build an SQL AST expression that matches
    // this template
    let expr: Expr | nil;
    for (const [k, v] of Object.entries(template ?? {})) {
        let right: Expr;
        if (nullIsh(v)) {
            // handle { myprop: null }
            right = {
                type: 'unary',
                op: 'IS NULL',
                operand: {
                    type: 'ref',
                    name: k,
                },
            };
        } else {
            let value: Expr;
            let op: BinaryOperator = '=';
            switch (typeof v) {
                case 'number':
                    // handle {myprop: 42}
                    value = Number.isInteger(v)
                        ? { type: 'integer', value: v }
                        : { type: 'numeric', value: v };
                    break;
                case 'string':
                    // handle {myprop: 'blah'}
                    value = { type: 'string', value: v };
                    break;
                case 'object':
                    // handle {myprop: new Date()}
                    if (moment.isMoment(v)) {
                        value = { type: 'string', value: v.toISOString() };
                    } else if (v instanceof Date) {
                        value = { type: 'string', value: moment(v).toISOString() };
                    } else {
                        // handle {myprop: {obj: "test"}}
                        op = '@>';
                        value = {
                            type: 'string',
                            value: JSON.stringify(v),
                        };
                    }
                    break;
                default:
                    throw new Error(`Object type of property "${k}" not supported in template`);
            }
            right = {
                type: 'binary',
                op,
                left: {
                    type: 'ref',
                    name: k,
                },
                right: value
            };
        }
        expr = !expr ? right : {
            type: 'binary',
            op: 'AND',
            left: expr,
            right,
        };
    }

    // === perform filter
    let ret = selection
        .filter(expr);
    if (columns) {
        ret = ret.select(columns.map<SelectedColumn>(x => ({
            expr: { type: 'ref', name: x as string },
        })));
    }
    return ret.enumerate(t);
}


function ver(v: string) {
    if (!v || !/^\d+(\.\d+)+$/.test(v)) {
        throw new Error('Invalid semver ' + v)
    }
    return v.split(/\./g).map(x => parseInt(x, 10));
}
export function compareVersions(_a: string, _b: string): number {
    const a = ver(_a);
    const b = ver(_b);
    const m = Math.max(a.length, b.length);
    for (let i = 0; i < m; i++) {
        const d = (b[i] || 0) - (a[i] || 0);
        if (d !== 0) {
            return d;
        }
    }
    return 0;
}


export function intervalToSec(v: Interval) {
    return (v.milliseconds ?? 0) / 1000
        + (v.seconds ?? 0)
        + (v.minutes ?? 0) * 60
        + (v.hours ?? 0) * 3600
        + (v.days ?? 0) * 3600 * 24
        + (v.months ?? 0) * 3600 * 24 * 30
        + (v.years ?? 0) * 3600 * 24 * 30 * 12;
}

export function parseRegClass(_reg: RegClass): QName | number {
    let reg = _reg;
    if (typeof reg === 'string' && /^\d+$/.test(reg)) {
        reg = parseInt(reg);
    }
    if (typeof reg === 'number') {
        return reg;
    }
    // todo remove casts after next pgsql-ast-parser release
    try {
        const ret = parse(reg, 'qualified_name' as any) as QName;
        return ret;
    } catch (e) {
        return { name: reg };
    }
}


const timeReg = /^(\d+):(\d+)(:(\d+))?(\.\d+)?$/;
export function parseTime(str: string): moment.Moment {
    const [_, a, b, __, c, d] = timeReg.exec(str) ?? [];
    if (!_) {
        throw new QueryError(`Invalid time format: ` + str);
    }
    const ms = d ? parseFloat(d) * 1000 : undefined;
    let ret: moment.Moment;
    if (c) {
        ret = moment.utc({
            h: parseInt(a, 10),
            m: parseInt(b, 10),
            s: parseInt(c, 10),
            ms,
        });
    } else {
        if (d) {
            ret = moment.utc({
                m: parseInt(a, 10),
                s: parseInt(b, 10),
                ms,
            });
        } else {
            ret = moment.utc({
                h: parseInt(a, 10),
                m: parseInt(b, 10),
                ms,
            });
        }
    }
    if (!ret.isValid()) {
        throw new QueryError(`Invalid time format: ` + str);
    }
    return ret;
}


export function colByName<T>(refs: Map<string, T>, ref: string | ExprRef, nullIfNotFound: boolean | nil): T | nil {
    const nm = typeof ref === 'string' ? ref
        : !ref.table ? ref.name
            : null;
    const got = nm ? refs.get(nm) : null;
    if (!got && !nullIfNotFound) {
        throw new ColumnNotFound(colToStr(ref));
    }
    return got;
}

export function colToStr(col: string | ExprRef) {
    if (typeof col === 'string') {
        return col;
    }
    if (!col.table) {
        return col.name;
    }
    return col.table.name + '.' + col.name;
}

export function qnameToStr(col: string | QName) {
    if (typeof col === 'string') {
        return col;
    }
    if (!col.schema) {
        return col.name;
    }
    return col.schema + '.' + col.name;
}

export function asSingleName(col: string | ExprRef): string | nil {
    if (typeof col === 'string') {
        return col;
    }
    if (col.table) {
        return null;
    }
    return col.name;
}

export function asSingleQName(col: string | QName, allowedSchema?: string): string | nil {
    if (typeof col === 'string') {
        return col;
    }
    if (col.schema && col.schema !== allowedSchema) {
        return null;
    }
    return col.name;
}

export function errorMessage(error: unknown): string {
    if (typeof error === 'string') {
        return error;
    }
    if (typeof error !== 'object') {
        return 'Unkown error message';
    }
    return (error as any)?.message;
}

export function it<T>(iterable: Iterable<T>): IteratorHelper<T> {
    return iterable instanceof IteratorHelper
        ? iterable as any
        : new IteratorHelper(() => iterable);
}

export class IteratorHelper<T> implements Iterable<T> {
    constructor(private underlying: () => Iterable<T>) { }

    [Symbol.iterator]() {
        return this.underlying()[Symbol.iterator]();
    }

    flatten(): T extends Iterable<infer X> ? IteratorHelper<X> : never {
        const that = this;
        function* wrap() {
            for (const v of that.underlying() as any ?? []) {
                for (const x of v) {
                    yield x;
                }
            }
        }
        return new IteratorHelper(wrap) as any;
    }

    reduce<U>(callbackfn: (previousValue: U, currentValue: T, currentIndex: number) => U, initialValue: U): U {
        let acc = initialValue;
        let i = 0;
        for (const v of this.underlying()) {
            acc = callbackfn(acc, v, i);
            i++;
        }
        return acc;
    }
}

export function fromEntries<K, V>(iterable: readonly (readonly [K, V])[]): Map<K, V> {
    const ret = new Map<K, V>();
    for (const [k, v] of iterable) {
        ret.set(k, v);
    }
    return ret;
}

export function notNil<T>(value: (T | nil)[] | nil): Exclude<T, null>[] {
    return (value ?? []).filter((x) => !nullIsh(x)) as any[];
}

/** Modify an array if necessary */
export function modifyIfNecessary<T>(values: T[], mapper: (input: T) => T | nil): T[] {
    let ret: T[] | undefined;
    for (let i = 0; i < values.length; i++) {
        const mapped = mapper(values[i]);
        if (nullIsh(mapped)) {
            continue;
        }
        if (!ret) {
            ret = [...values];
        }
        ret[i] = mapped;
    }
    return ret ?? values;
}
