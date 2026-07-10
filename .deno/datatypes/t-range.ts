import { DataType, nil, _IType, _ISchema } from '../interfaces-private.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../evaluator.ts';
import { Types } from './datatypes.ts';
import { QueryError } from '../interfaces.ts';
import { nullIsh } from '../utils.ts';

/** Per-range-type behaviour: how to compare, format and (for discrete types) step bounds. */
export interface RangeAdapter {
    name: string;
    /** element (subtype) type name, e.g. 'int4' */
    elem: () => _IType;
    discrete: boolean;
    /** comparison key for a bound's text */
    key(boundText: string): number;
    /** element raw value (as produced by the engine) -> bound text */
    fromRaw(raw: any): string;
    /** normalize a bound parsed from a literal -> canonical text */
    normText(boundText: string): string;
    /** bound text -> element raw value (for lower()/upper()) */
    toRaw(boundText: string): any;
    /** discrete types only: bound text -> next value's text */
    stepUp?(boundText: string): string;
}

interface Bounds {
    empty: boolean;
    loInc: boolean;
    hiInc: boolean;
    lo: string | null; // null = unbounded
    hi: string | null;
}

function fmtDate(d: Date): string {
    const y = d.getUTCFullYear().toString().padStart(4, '0');
    const m = (d.getUTCMonth() + 1).toString().padStart(2, '0');
    const day = d.getUTCDate().toString().padStart(2, '0');
    return `${y}-${m}-${day}`;
}

function toDate(text: string): Date {
    return new Date(text.trim().length <= 10 ? text.trim() + 'T00:00:00Z' : text.trim());
}

const INT = (name: string, elem: () => _IType, big: boolean): RangeAdapter => ({
    name,
    elem,
    discrete: true,
    key: t => Number(t),
    fromRaw: r => (big ? BigInt(typeof r === 'string' ? r : Math.trunc(r)).toString() : String(Math.trunc(Number(r)))),
    normText: t => (big ? BigInt(t.trim()).toString() : String(parseInt(t.trim(), 10))),
    toRaw: t => (big ? t : parseInt(t, 10)),
    stepUp: t => (big ? (BigInt(t) + BigInt(1)).toString() : String(parseInt(t, 10) + 1)),
});

export const RANGE_ADAPTERS: RangeAdapter[] = [
    INT('int4range', () => Types.integer, false),
    INT('int8range', () => Types.bigint, true),
    {
        name: 'numrange',
        elem: () => Types.decimal(),
        discrete: false,
        key: t => parseFloat(t),
        fromRaw: r => String(r),
        normText: t => t.trim(),
        toRaw: t => t,
    },
    {
        name: 'daterange',
        elem: () => Types.date,
        discrete: true,
        key: t => toDate(t).getTime(),
        fromRaw: r => fmtDate(r instanceof Date ? r : new Date(r)),
        normText: t => fmtDate(toDate(t)),
        toRaw: t => toDate(t),
        stepUp: t => { const d = toDate(t); d.setUTCDate(d.getUTCDate() + 1); return fmtDate(d); },
    },
    {
        name: 'tsrange',
        elem: () => Types.timestamp(),
        discrete: false,
        key: t => new Date(t.trim()).getTime(),
        fromRaw: r => (r instanceof Date ? r.toISOString() : String(r)),
        normText: t => t.trim(),
        toRaw: t => new Date(t.trim()),
    },
    {
        name: 'tstzrange',
        elem: () => Types.timestamptz(),
        discrete: false,
        key: t => new Date(t.trim()).getTime(),
        fromRaw: r => (r instanceof Date ? r.toISOString() : String(r)),
        normText: t => t.trim(),
        toRaw: t => new Date(t.trim()),
    },
];

/** Parse a range literal like `[1,10)`, `(,5]`, `empty` into raw bounds. */
export function parseRangeLiteral(text: string): Bounds {
    const t = text.trim();
    if (t.toLowerCase() === 'empty') {
        return { empty: true, loInc: false, hiInc: false, lo: null, hi: null };
    }
    const first = t[0];
    const last = t[t.length - 1];
    if ((first !== '[' && first !== '(') || (last !== ']' && last !== ')')) {
        throw new QueryError(`malformed range literal: "${text}"`, '22P02');
    }
    const inner = t.slice(1, -1);
    const comma = inner.indexOf(',');
    if (comma < 0) {
        throw new QueryError(`malformed range literal: "${text}"`, '22P02');
    }
    const loStr = inner.slice(0, comma).trim();
    const hiStr = inner.slice(comma + 1).trim();
    return {
        empty: false,
        loInc: first === '[',
        hiInc: last === ']',
        lo: loStr === '' ? null : unquote(loStr),
        hi: hiStr === '' ? null : unquote(hiStr),
    };
}

function unquote(s: string): string {
    if (s.length >= 2 && s[0] === '"' && s[s.length - 1] === '"') {
        return s.slice(1, -1);
    }
    return s;
}

/** Canonicalize bounds and render to the canonical text form Postgres would emit. */
export function canonicalize(a: RangeAdapter, b: Bounds): string {
    if (b.empty) {
        return 'empty';
    }
    let { lo, hi, loInc, hiInc } = b;
    if (lo !== null) { lo = a.normText(lo); }
    if (hi !== null) { hi = a.normText(hi); }

    if (a.discrete && a.stepUp) {
        // discrete ranges are canonicalized to `[lower, upper)`
        if (lo !== null && !loInc) { lo = a.stepUp(lo); loInc = true; }
        if (hi !== null && hiInc) { hi = a.stepUp(hi); hiInc = false; }
    }

    if (lo !== null && hi !== null) {
        const lk = a.key(lo), hk = a.key(hi);
        if (lk > hk) {
            throw new QueryError('range lower bound must be less than or equal to range upper bound', '22000');
        }
        // empty when the bounds collapse
        if (lk === hk && !(loInc && hiInc)) {
            return 'empty';
        }
    }
    return (loInc ? '[' : '(') + (lo ?? '') + ',' + (hi ?? '') + (hiInc ? ']' : ')');
}

interface KeyedBounds {
    empty: boolean;
    loInc: boolean;
    hiInc: boolean;
    loKey: number; // -Infinity if unbounded
    hiKey: number; // +Infinity if unbounded
}

export function toKeyed(a: RangeAdapter, canonical: string): KeyedBounds {
    const b = parseRangeLiteral(canonical);
    return {
        empty: b.empty,
        loInc: b.loInc,
        hiInc: b.hiInc,
        loKey: b.lo === null ? -Infinity : a.key(b.lo),
        hiKey: b.hi === null ? Infinity : a.key(b.hi),
    };
}

// -- bound comparisons ---------------------------------------------------------

/** true if A's lower bound is <= B's lower bound (A starts at or before B) */
function lowerLE(a: KeyedBounds, b: KeyedBounds): boolean {
    if (a.loKey !== b.loKey) { return a.loKey < b.loKey; }
    return a.loInc || !b.loInc; // inclusive lower starts earlier-or-equal
}
/** true if A's upper bound is >= B's upper bound (A ends at or after B) */
function upperGE(a: KeyedBounds, b: KeyedBounds): boolean {
    if (a.hiKey !== b.hiKey) { return a.hiKey > b.hiKey; }
    return a.hiInc || !b.hiInc;
}

export function rangeContainsRange(a: RangeAdapter, aStr: string, bStr: string): boolean {
    const B = toKeyed(a, bStr);
    if (B.empty) { return true; }
    const A = toKeyed(a, aStr);
    if (A.empty) { return false; }
    return lowerLE(A, B) && upperGE(A, B);
}

export function rangeContainsElem(a: RangeAdapter, rStr: string, elemKey: number): boolean {
    const R = toKeyed(a, rStr);
    if (R.empty) { return false; }
    const lo = elemKey > R.loKey || (elemKey === R.loKey && R.loInc);
    const hi = elemKey < R.hiKey || (elemKey === R.hiKey && R.hiInc);
    return lo && hi;
}

export function rangesOverlap(a: RangeAdapter, aStr: string, bStr: string): boolean {
    const A = toKeyed(a, aStr);
    const B = toKeyed(a, bStr);
    if (A.empty || B.empty) { return false; }
    // A entirely before B ?
    const aBeforeB = A.hiKey < B.loKey || (A.hiKey === B.loKey && !(A.hiInc && B.loInc));
    const bBeforeA = B.hiKey < A.loKey || (B.hiKey === A.loKey && !(B.hiInc && A.loInc));
    return !aBeforeB && !bBeforeA;
}

/** A named range type. A value is stored as its canonical text form (or null). */
export class RangeType extends TypeBase<string> {

    get primary(): DataType {
        return this.adapter.name as any;
    }

    get name(): string {
        return this.adapter.name;
    }

    constructor(readonly schema: _ISchema, readonly adapter: RangeAdapter) {
        super(null);
    }

    install(): this {
        this.schema._registerType(this);
        return this;
    }

    doCanCast(to: _IType): boolean | nil {
        return to.primary === DataType.text;
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        // stored form is already the canonical text
        return a.setType(to);
    }

    doCanBuildFrom(from: _IType): boolean | nil {
        return from.primary === DataType.text;
    }

    doBuildFrom(value: Evaluator, from: _IType): Evaluator | nil {
        if (from.primary !== DataType.text) { return null; }
        const adapter = this.adapter;
        return value.setConversion(
            (raw: string) => nullIsh(raw) ? null : canonicalize(adapter, parseRangeLiteral(raw)),
            toRange => ({ toRange: adapter.name, v: toRange }),
        ).setType(this);
    }

    doEquals(a: string, b: string): boolean {
        return a === b; // canonical forms
    }
}

export function asRange(t: _IType | nil): RangeType | null {
    return t instanceof RangeType ? t : null;
}
