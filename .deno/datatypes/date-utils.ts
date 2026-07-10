// Zero-dependency date/time helpers — replaces the (heavy) `moment` dependency.
// Everything is UTC: pg-mem stores timestamps as UTC `Date` objects, and Postgres
// timestamp text without a zone is interpreted as UTC here.

const DAY_MS = 86_400_000;

const MONTHS_LONG = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
const MONTHS_SHORT = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
const DAYS_LONG = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
const DAYS_SHORT = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

const two = (n: number) => String(n).padStart(2, '0');

/** ISO-8601 week number + week-numbering year for a UTC date. */
function isoWeekParts(d: Date): { week: number; year: number } {
    const date = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
    const dayNum = date.getUTCDay() === 0 ? 7 : date.getUTCDay();
    date.setUTCDate(date.getUTCDate() + 4 - dayNum); // to the Thursday of this week
    const year = date.getUTCFullYear();
    const yearStart = Date.UTC(year, 0, 1);
    const week = Math.ceil(((date.getTime() - yearStart) / DAY_MS + 1) / 7);
    return { week, year };
}

/** Parse a timestamp string leniently, interpreting a naive (zone-less) value as UTC. */
function parseLenient(str: string): Date {
    const s = str.trim();
    if (/^\d{4}-\d{1,2}-\d{1,2}$/.test(s)) {
        return new Date(s + 'T00:00:00Z');
    }
    const m = /^(\d{4}-\d{1,2}-\d{1,2})[ T](\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?)(Z|[+-]\d{2}(?::?\d{2})?)?$/.exec(s);
    if (m) {
        let tz = m[3] ?? 'Z'; // naive => UTC
        if (/^[+-]\d{2}$/.test(tz)) { tz += ':00'; } // JS needs ±HH:MM, not ±HH
        return new Date(`${m[1]}T${m[2]}${tz}`);
    }
    return new Date(s); // fall back to the JS parser (ISO etc.)
}

/** A tiny moment-like accessor wrapper over a UTC `Date`. All getters are UTC. */
export class UtcDate {
    readonly d: Date;
    constructor(input: Date | number | string | UtcDate) {
        this.d = input instanceof UtcDate ? input.d
            : input instanceof Date ? input
                : typeof input === 'number' ? new Date(input)
                    : parseLenient(input);
    }
    isValid() { return !isNaN(this.d.getTime()); }
    year() { return this.d.getUTCFullYear(); }
    month() { return this.d.getUTCMonth(); } // 0-11
    date() { return this.d.getUTCDate(); }
    hour() { return this.d.getUTCHours(); }
    hours() { return this.d.getUTCHours(); }
    minute() { return this.d.getUTCMinutes(); }
    minutes() { return this.d.getUTCMinutes(); }
    second() { return this.d.getUTCSeconds(); }
    seconds() { return this.d.getUTCSeconds(); }
    millisecond() { return this.d.getUTCMilliseconds(); }
    milliseconds() { return this.d.getUTCMilliseconds(); }
    day() { return this.d.getUTCDay(); } // 0=Sunday
    valueOf() { return this.d.getTime(); }
    unix() { return Math.floor(this.d.getTime() / 1000); }
    toDate() { return this.d; }
    toISOString() { return this.d.toISOString(); }
    quarter() { return Math.floor(this.d.getUTCMonth() / 3) + 1; }
    daysInMonth() { return new Date(Date.UTC(this.year(), this.month() + 1, 0)).getUTCDate(); }
    dayOfYear() { return Math.floor((this.d.getTime() - Date.UTC(this.year(), 0, 1)) / DAY_MS) + 1; }
    isoWeekday() { return this.d.getUTCDay() === 0 ? 7 : this.d.getUTCDay(); } // 1=Mon..7=Sun
    isoWeek() { return isoWeekParts(this.d).week; }
    isoWeekYear() { return isoWeekParts(this.d).year; }
    week() { return this.isoWeek(); } // pg EXTRACT(week) / date_part('week') is the ISO week
    format(token: string): string {
        switch (token) {
            case 'MMMM': return MONTHS_LONG[this.month()];
            case 'MMM': return MONTHS_SHORT[this.month()];
            case 'dddd': return DAYS_LONG[this.day()];
            case 'ddd': return DAYS_SHORT[this.day()];
            case 'HH:mm:ss': return `${two(this.hour())}:${two(this.minute())}:${two(this.second())}`;
            default: throw new Error('unsupported format token: ' + token);
        }
    }
    diff(other: UtcDate | Date, unit?: 'days' | 'seconds' | 'milliseconds'): number {
        const b = other instanceof UtcDate ? other.d : other;
        const ms = this.d.getTime() - b.getTime();
        if (unit === 'days') { return Math.trunc(ms / DAY_MS); }
        if (unit === 'seconds') { return Math.trunc(ms / 1000); }
        return ms;
    }
}

/** `utc(x)` mirrors `moment.utc(x)` for the value-wrapping cases. */
export function utc(input: Date | number | string | UtcDate): UtcDate {
    return new UtcDate(input);
}

/** Construct a UTC Date from calendar parts, or `null` if any field is out of range
 * (native Date rolls over, e.g. month 13 → next year; Postgres treats that as invalid). */
export function fromParts(p: { year: number; month: number; day: number; hour?: number; minute?: number; second?: number; millisecond?: number }): Date | null {
    const { year, month, day, hour = 0, minute = 0, second = 0, millisecond = 0 } = p;
    const d = new Date(Date.UTC(year, month, day, hour, minute, second, millisecond));
    if (isNaN(d.getTime())
        || d.getUTCFullYear() !== year || d.getUTCMonth() !== month || d.getUTCDate() !== day
        || d.getUTCHours() !== hour || d.getUTCMinutes() !== minute || d.getUTCSeconds() !== second) {
        return null;
    }
    return d;
}

/** Truncate a UTC date to the start of the given unit (date_trunc). */
export function startOf(d: Date, unit: 'year' | 'quarter' | 'month' | 'isoWeek' | 'day' | 'hour' | 'minute' | 'second' | 'millisecond'): Date {
    const y = d.getUTCFullYear(), mo = d.getUTCMonth(), da = d.getUTCDate();
    const h = d.getUTCHours(), mi = d.getUTCMinutes(), s = d.getUTCSeconds();
    switch (unit) {
        case 'year': return new Date(Date.UTC(y, 0, 1));
        case 'quarter': return new Date(Date.UTC(y, Math.floor(mo / 3) * 3, 1));
        case 'month': return new Date(Date.UTC(y, mo, 1));
        case 'isoWeek': {
            const dow = d.getUTCDay() === 0 ? 7 : d.getUTCDay(); // Monday-based
            return new Date(Date.UTC(y, mo, da) - (dow - 1) * DAY_MS);
        }
        case 'day': return new Date(Date.UTC(y, mo, da));
        case 'hour': return new Date(Date.UTC(y, mo, da, h));
        case 'minute': return new Date(Date.UTC(y, mo, da, h, mi));
        case 'second': return new Date(Date.UTC(y, mo, da, h, mi, s));
        case 'millisecond': return new Date(d.getTime());
    }
}

export function addDays(d: Date, n: number): Date {
    return new Date(d.getTime() + n * DAY_MS);
}

export function addMilliseconds(d: Date, n: number): Date {
    return new Date(d.getTime() + n);
}

/** Add calendar months, clamping the day to the target month's length
 * (Jan 31 + 1 month = Feb 28), matching Postgres interval arithmetic. */
export function addMonths(d: Date, n: number): Date {
    const total = d.getUTCMonth() + n;
    const ty = d.getUTCFullYear() + Math.floor(total / 12);
    const tm = ((total % 12) + 12) % 12;
    const dim = new Date(Date.UTC(ty, tm + 1, 0)).getUTCDate();
    const day = Math.min(d.getUTCDate(), dim);
    return new Date(Date.UTC(ty, tm, day, d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds(), d.getUTCMilliseconds()));
}

// ---- format-directed parsing for to_date / to_timestamp(text, format) ----

interface ParseTok { re: string; set?: (v: number, into: FieldAcc) => void; }
interface FieldAcc { year: number; month: number; day: number; hour: number; minute: number; second: number; ms: number; }

const NAMED_MONTHS = MONTHS_LONG.map((m, i) => ({ long: m.toLowerCase(), short: MONTHS_SHORT[i].toLowerCase(), i }));

// tokens, longest-pattern first
const PARSE_TOKENS: { pat: string; tok: (acc: FieldAcc) => ParseTok }[] = [
    { pat: 'YYYY', tok: () => ({ re: '(\\d{4})', set: (v, a) => a.year = v }) },
    { pat: 'HH24', tok: () => ({ re: '(\\d{1,2})', set: (v, a) => a.hour = v }) },
    { pat: 'HH12', tok: () => ({ re: '(\\d{1,2})', set: (v, a) => a.hour = v }) },
    { pat: 'MON', tok: () => monthNameTok() },
    { pat: 'YYY', tok: () => ({ re: '(\\d{3})', set: (v, a) => a.year = 2000 + v }) },
    { pat: 'YY', tok: () => ({ re: '(\\d{2})', set: (v, a) => a.year = 2000 + v }) },
    { pat: 'MM', tok: () => ({ re: '(\\d{1,2})', set: (v, a) => a.month = v - 1 }) },
    { pat: 'DD', tok: () => ({ re: '(\\d{1,2})', set: (v, a) => a.day = v }) },
    { pat: 'HH', tok: () => ({ re: '(\\d{1,2})', set: (v, a) => a.hour = v }) },
    { pat: 'MI', tok: () => ({ re: '(\\d{1,2})', set: (v, a) => a.minute = v }) },
    { pat: 'SS', tok: () => ({ re: '(\\d{1,2})', set: (v, a) => a.second = v }) },
    { pat: 'MS', tok: () => ({ re: '(\\d{1,3})', set: (v, a) => a.ms = v }) },
];

function monthNameTok(): ParseTok {
    return { re: '([A-Za-z]+)' }; // handled specially (string, not number)
}

/** Parse `input` per a Postgres `to_date`/`to_timestamp` format string. Returns a UTC
 * Date, or `null` if it does not match. Supports the common numeric + month-name tokens. */
export function parseFormat(input: string, fmt: string): Date | null {
    // "MONTH"/"Month" are case-insensitive variants that reduce to the month-name token
    const norm = fmt.replace(/MONTH/gi, 'MON').replace(/Mon/g, 'MON');
    let re = '^';
    const setters: (((groups: string[], idx: { i: number }, acc: FieldAcc) => void))[] = [];
    let i = 0;
    let isMonthName = false;
    outer: while (i < norm.length) {
        for (const { pat, tok } of PARSE_TOKENS) {
            if (norm.startsWith(pat, i)) {
                const t = tok({} as FieldAcc);
                re += t.re;
                if (pat === 'MON') {
                    isMonthName = true;
                    setters.push((g, idx, a) => {
                        const name = g[idx.i++].toLowerCase();
                        const found = NAMED_MONTHS.find(m => m.long === name || m.short === name);
                        if (found) { a.month = found.i; }
                    });
                } else {
                    const set = t.set!;
                    setters.push((g, idx, a) => set(parseInt(g[idx.i++], 10), a));
                }
                i += pat.length;
                continue outer;
            }
        }
        // literal char: match it, allowing flexible whitespace/separators
        const ch = norm[i++];
        re += /\s/.test(ch) ? '\\s+' : ch.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }
    re += '$';
    const flags = isMonthName ? 'i' : '';
    const m = new RegExp(re, flags).exec(input.trim());
    if (!m) { return null; }
    const acc: FieldAcc = { year: 1, month: 0, day: 1, hour: 0, minute: 0, second: 0, ms: 0 };
    const idx = { i: 1 };
    for (const s of setters) { s(m as unknown as string[], idx, acc); }
    return fromParts({ year: acc.year, month: acc.month, day: acc.day, hour: acc.hour, minute: acc.minute, second: acc.second, millisecond: acc.ms });
}
