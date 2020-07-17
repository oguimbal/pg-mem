import moment from 'moment';
import { Stack } from 'immutable';
import { IValue, NotSupported } from './interfaces-private';

export interface Ctor<T> extends Function {
    new(...params: any[]): T; prototype: T;
}



export function trimNullish<T>(value: T, depth = 5): T {
    if (depth < 0)
        return;
    if (value instanceof Array) {
        value.forEach(x => trimNullish(x, depth - 1))
    }
    if (typeof value !== 'object' || value instanceof Date || moment.isMoment(value) || moment.isDuration(value))
        return;

    if (!value) {
        return value;
    }

    for (const k of Object.keys(value)) {
        const val = value[k];
        if (val === undefined || val === null)
            delete value[k];
        else
            trimNullish(val, depth - 1);
    }
    return value;
}


export function watchUse<T>(value: T, stack: Stack<string> = Stack()): T & { check?(); } {
    if (!value || globalThis?.process?.env?.['NOCHECKFULLQUERYUSAGE'] === 'true') {
        return value;
    }
    if (typeof value !== 'object') {
        throw new NotSupported();
    }
    const ret = {};
    const toUse = new Map<string, any>();
    const watchables: { check(); }[] = [];
    for (const [k, _v] of Object.entries(value)) {
        let v = _v;
        if (Array.isArray(v)) {
            const trans = [];
            for (const x of v) {
                if (typeof x === 'object' && x) {
                    const w = watchUse(x, stack.push(`[${trans.length}]`));
                    watchables.push(w);
                    trans.push(w);
                } else {
                    trans.push(x);
                }
            }
            v = trans;
        } else if (typeof v === 'object' && v) {
            v = watchUse(v, stack.push('.' + k));
            watchables.push(v);
        }
        if (v === null || v === undefined) {
            continue;
        }
        toUse.set(k, v);
        Object.defineProperty(ret, k, {
            get() {
                toUse.delete(k);
                return v;
            },
            enumerable: true,
        });
    }
    ret['check'] = function () {
        if (toUse.size) {
            const st = stack.join('');
            throw new NotSupported('query parts ' + [...toUse.entries()]
                .map(([k, v]) => st + '.' + k + ' (' + JSON.stringify(v) + ')')
                .join(', '));
        }
        for (const w of watchables) {
            w?.check();
        }
    }
    return ret as any;
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
            return 1;
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
    if (fa || fb) {
        if (Math.abs(<any>a - <any>b) <= numberDelta) {
            return 0;
        }
        return a > b ? 1 : -1;
    }

    // === handle plain objects
    if (typeof a !== 'object') {
        return -1; // objects are at the end
    }
    if (typeof b !== 'object') {
        return 1; // objects are at the end
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
        const inner = deepCompare(a[k], b[k], strict, depth - 1, numberDelta);
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

export function buildColumnIds(columns: IValue[]) {
    const exists = new Set(columns.map(x => x.id));
    const got = new Set();
    let cid = 0;
    return columns
        .map(x => {
            if (x.id && !got.has(x.id)) {
                got.add(x.id);
                return x.id;
            }
            let base = x.id ?? 'column';
            let nm: string;
            do {
                nm = base + (cid++);
            } while (exists.has(nm) || got.has(nm));
            got.add(nm);
            return nm;
        });
}