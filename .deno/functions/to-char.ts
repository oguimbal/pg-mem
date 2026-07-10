import { QueryError } from '../interfaces-private.ts';
import moment from 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts';

// to_char() formatting - the commonly used subset, verified against postgres 16.

// ============================ numeric formatting ==================================

export function numberToChar(value: number, fmt: string): string {
    let fm = false;
    let pattern = fmt;
    if (/^FM/i.test(pattern)) {
        fm = true;
        pattern = pattern.slice(2);
    }
    let miSign = false;
    if (/MI$/i.test(pattern)) {
        miSign = true;
        pattern = pattern.slice(0, -2);
    }
    // pull out quoted literals: each remembers which pattern-char position it precedes
    const literals: { at: number; text: string }[] = [];
    let stripped = '';
    for (let i = 0; i < pattern.length;) {
        if (pattern[i] === '"') {
            const end = pattern.indexOf('"', i + 1);
            literals.push({ at: stripped.length, text: pattern.slice(i + 1, end < 0 ? undefined : end) });
            i = end < 0 ? pattern.length : end + 1;
        } else {
            stripped += pattern[i++];
        }
    }
    // V multiplies by 10^n instead of rendering a decimal point
    const vAt = stripped.search(/V/i);
    if (vAt >= 0) {
        const shift = stripped.slice(vAt + 1).replace(/[^90]/g, '').length;
        value = value * 10 ** shift;
        stripped = stripped.slice(0, vAt) + stripped.slice(vAt + 1);
    }
    const dotAt = stripped.search(/[.D]/i);
    const intPat = dotAt < 0 ? stripped : stripped.slice(0, dotAt);
    const fracPat = dotAt < 0 ? '' : stripped.slice(dotAt + 1);
    if (!/^[90,GS]*$/i.test(intPat) || !/^[90]*$/.test(fracPat)) {
        throw new QueryError(`unsupported to_char number format "${fmt}"`);
    }
    const explicitSign = /S/i.test(intPat);

    const fracSlots = fracPat.length;
    const neg = value < 0;
    const rounded = Math.abs(value).toFixed(fracSlots);
    const [intDigitsRaw, fracDigits = ''] = rounded.split('.');
    // a zero integer part renders as blank digits (pg: to_char(0.1, '9.9') = ' .1')
    // ... except in fill mode when the whole value is zero (pg: to_char(0, 'FM9.99') = '0.')
    const zeroKept = fm && +rounded === 0;
    const intDigits = intDigitsRaw === '0' && fracSlots && !zeroKept ? '' : intDigitsRaw;

    // a '0' slot forces zero-fill for every slot at or right of it
    const leftmostZero = intPat.indexOf('0');

    // fill integer slots right-to-left
    const out: string[] = [];
    let d = intDigits.length - 1;
    let overflow = false;
    for (let i = intPat.length - 1; i >= 0; i--) {
        const c = intPat[i];
        if (c === '9' || c === '0') {
            if (d >= 0) {
                out.unshift(intDigits[d--]);
            } else {
                out.unshift(leftmostZero >= 0 && i >= leftmostZero ? '0' : ' ');
            }
        } else if (c === ',' || c.toUpperCase() === 'G') {
            out.unshift(',');
        } else {
            // sign slot: resolved below
            out.unshift('S');
        }
    }
    if (d >= 0) {
        overflow = true;
        // does not fit: pg turns digit slots into '#', keeping separators and the sign column
        for (let i = 0; i < out.length; i++) {
            if (out[i] !== ',') {
                out[i] = '#';
            }
        }
    }
    // separators floating in the space-padded area become spaces
    for (let i = 0; i < out.length; i++) {
        if (out[i] === ',' && (i === 0 || out[i - 1] === ' ')) {
            out[i] = ' ';
        }
    }
    let ret = out.join('');
    if (explicitSign && !overflow) {
        ret = ret.replace('S', neg ? '-' : '+');
    }

    if (fracSlots) {
        let frac = overflow ? '#'.repeat(fracSlots) : fracDigits.padEnd(fracSlots, '0');
        if (fm && !overflow) {
            // FM drops trailing zeros in '9' slots (but keeps '0'-forced ones)
            let keep = fracSlots;
            while (keep > 0 && fracPat[keep - 1] === '9' && frac[keep - 1] === '0') {
                keep--;
            }
            frac = frac.slice(0, keep);
        }
        if (frac.length || !fm || zeroKept) {
            ret += '.' + frac;
        }
    }

    // re-insert quoted literals at their pattern positions (sign column comes after
    // a leading literal, so offset by 1 for literals past position 0)
    if (!explicitSign && !miSign) {
        ret = (neg ? '-' : ' ') + ret;
        for (let i = literals.length - 1; i >= 0; i--) {
            const { at, text } = literals[i];
            const pos = at === 0 ? 0 : at + 1;
            ret = ret.slice(0, pos) + text + ret.slice(pos);
        }
    } else {
        for (let i = literals.length - 1; i >= 0; i--) {
            ret = ret.slice(0, literals[i].at) + literals[i].text + ret.slice(literals[i].at);
        }
        if (miSign) {
            ret += neg ? '-' : ' ';
        }
    }
    if (fm) {
        ret = ret.trimStart();
        if (miSign) {
            ret = ret.trimEnd();
        }
    }
    return ret;
}

// ============================ date/time formatting ================================

interface DateToken {
    /** pattern, longest first */
    pat: string;
    fmt: (m: moment.Moment, fm: boolean) => string;
}

function padName(name: string, fm: boolean): string {
    return fm ? name : name.padEnd(9, ' ');
}

function caseAs(pattern: string, name: string): string {
    if (pattern === pattern.toUpperCase()) {
        return name.toUpperCase();
    }
    if (pattern === pattern.toLowerCase()) {
        return name.toLowerCase();
    }
    return name;
}

const two = (n: number) => String(n).padStart(2, '0');

const DATE_TOKENS: DateToken[] = [
    { pat: 'Y,YYY', fmt: m => String(m.year()).padStart(4, '0').replace(/(\d)(\d{3})$/, '$1,$2') },
    { pat: 'IYYY', fmt: m => String(m.isoWeekYear()).padStart(4, '0') },
    { pat: 'IYY', fmt: m => String(m.isoWeekYear() % 1000).padStart(3, '0') },
    { pat: 'IY', fmt: m => two(m.isoWeekYear() % 100) },
    { pat: 'HH24', fmt: m => two(m.hour()) },
    { pat: 'HH12', fmt: m => two(m.hour() % 12 || 12) },
    { pat: 'YYYY', fmt: m => String(m.year()).padStart(4, '0') },
    { pat: 'DDD', fmt: m => String(m.dayOfYear()).padStart(3, '0') },
    ...['MONTH', 'Month', 'month'].map<DateToken>(pat => ({
        pat,
        fmt: (m, fm) => padName(caseAs(pat, m.format('MMMM')), fm),
    })),
    ...['MON', 'Mon', 'mon'].map<DateToken>(pat => ({
        pat,
        fmt: m => caseAs(pat, m.format('MMM')),
    })),
    ...['DAY', 'Day', 'day'].map<DateToken>(pat => ({
        pat,
        fmt: (m, fm) => padName(caseAs(pat, m.format('dddd')), fm),
    })),
    ...['DY', 'Dy', 'dy'].map<DateToken>(pat => ({
        pat,
        fmt: m => caseAs(pat, m.format('ddd')),
    })),
    { pat: 'HH', fmt: m => two(m.hour() % 12 || 12) },
    { pat: 'MI', fmt: m => two(m.minute()) },
    { pat: 'SS', fmt: m => two(m.second()) },
    { pat: 'MS', fmt: m => String(m.millisecond()).padStart(3, '0') },
    { pat: 'US', fmt: m => String(m.millisecond() * 1000).padStart(6, '0') },
    { pat: 'YYY', fmt: m => String(m.year() % 1000).padStart(3, '0') },
    { pat: 'YY', fmt: m => two(m.year() % 100) },
    { pat: 'MM', fmt: m => two(m.month() + 1) },
    { pat: 'DD', fmt: m => two(m.date()) },
    { pat: 'ID', fmt: m => String(m.isoWeekday()) },
    { pat: 'IW', fmt: m => two(m.isoWeek()) },
    { pat: 'WW', fmt: m => two(Math.ceil(m.dayOfYear() / 7)) },
    { pat: 'CC', fmt: m => two(Math.ceil(m.year() / 100)) },
    ...['AM', 'PM'].map<DateToken>(pat => ({ pat, fmt: m => m.hour() < 12 ? 'AM' : 'PM' })),
    ...['am', 'pm'].map<DateToken>(pat => ({ pat, fmt: m => m.hour() < 12 ? 'am' : 'pm' })),
    { pat: 'D', fmt: m => String(m.day() + 1) },
    { pat: 'Q', fmt: m => String(m.quarter()) },
    { pat: 'TZ', fmt: () => '' },
    { pat: 'OF', fmt: () => '+00' },
    { pat: 'J', fmt: m => String(m.diff(moment.utc('-4713-11-24', 'Y-MM-DD'), 'days')) },
];

// interval fields map directly (no date anchoring: 'DD' of '3 days' is '03')
const INTERVAL_TOKENS: { pat: string; get: (v: any) => string }[] = [
    { pat: 'YYYY', get: v => String(v.years ?? 0).padStart(4, '0') },
    { pat: 'HH24', get: v => two(v.hours ?? 0) },
    { pat: 'HH12', get: v => two((v.hours ?? 0) % 12 || 12) },
    { pat: 'MS', get: v => String(v.milliseconds ?? 0).padStart(3, '0') },
    { pat: 'HH', get: v => two((v.hours ?? 0) % 12 || 12) },
    { pat: 'MI', get: v => two(v.minutes ?? 0) },
    { pat: 'SS', get: v => two(v.seconds ?? 0) },
    { pat: 'MM', get: v => two(v.months ?? 0) },
    { pat: 'DD', get: v => two(v.days ?? 0) },
];

export function intervalToChar(value: any, fmt: string): string {
    let ret = '';
    let i = 0;
    outer: while (i < fmt.length) {
        if (fmt[i] === '"') {
            const end = fmt.indexOf('"', i + 1);
            ret += fmt.slice(i + 1, end < 0 ? undefined : end);
            i = end < 0 ? fmt.length : end + 1;
            continue;
        }
        for (const tok of INTERVAL_TOKENS) {
            if (fmt.startsWith(tok.pat, i)) {
                ret += tok.get(value);
                i += tok.pat.length;
                continue outer;
            }
        }
        ret += fmt[i++];
    }
    return ret;
}

export function dateToChar(value: Date, fmt: string): string {
    const m = moment.utc(value);
    let ret = '';
    let fm = false;
    let i = 0;
    outer: while (i < fmt.length) {
        // quoted literal
        if (fmt[i] === '"') {
            const end = fmt.indexOf('"', i + 1);
            ret += fmt.slice(i + 1, end < 0 ? undefined : end);
            i = end < 0 ? fmt.length : end + 1;
            continue;
        }
        // FM (fill mode) applies to the next token only
        if (fmt.startsWith('FM', i) || fmt.startsWith('fm', i)) {
            fm = true;
            i += 2;
            continue;
        }
        for (const tok of DATE_TOKENS) {
            if (fmt.startsWith(tok.pat, i)) {
                let out = tok.fmt(m, fm);
                if (fm) {
                    out = out.replace(/^0+(?=\d)/, '');
                    fm = false;
                }
                ret += out;
                i += tok.pat.length;
                continue outer;
            }
        }
        ret += fmt[i++];
    }
    return ret;
}
