import { FunctionDefinition } from '../interfaces.ts';
import { utc, fromParts, startOf, parseFormat } from '../datatypes/date-utils.ts';
import { Interval } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { DataType, QueryError } from '../interfaces-private.ts';
import { nullIsh } from '../utils.ts';
import { dateToChar, intervalToChar, numberToChar } from './to-char.ts';


export const dateFunctions: FunctionDefinition[] = [
    {
        name: 'to_date',
        args: [DataType.text, DataType.text],
        returns: DataType.date,
        implementation: (data, format) => {
            if (nullIsh(data) || nullIsh(format)) {
                return null; // if one argument is null => null
            }
            const ret = parseFormat(data, format);
            if (!ret) {
                throw new QueryError(`The text '${data}' does not match the date format ${format}`);
            }
            return ret;
        }
    },
    {
        name: 'now',
        returns: DataType.timestamptz,
        impure: true,
        implementation: () => new Date(),
    },
    ...[
        { arg: DataType.timestamp, ret: DataType.timestamp },
        { arg: DataType.timestamptz, ret: DataType.timestamptz },
        { arg: DataType.date, ret: DataType.timestamp },
    ].map<FunctionDefinition>(({ arg, ret }) => ({
        name: 'date_trunc',
        args: [DataType.text, arg],
        returns: ret,
        implementation: (field: string, val: Date) => {
            const unit = dateTruncUnits[field?.toLowerCase()];
            if (!unit) {
                throw new QueryError(`unit "${field}" not supported for type timestamp`);
            }
            return startOf(val, unit);
        },
    })),
    ...[DataType.timestamp, DataType.timestamptz, DataType.date].map<FunctionDefinition>(arg => ({
        name: 'date_part',
        args: [DataType.text, arg],
        returns: DataType.float,
        implementation: (field: string, val: Date) => datePart(field, val),
    })),
    {
        name: 'to_timestamp',
        args: [DataType.float],
        returns: DataType.timestamptz,
        allowNullArguments: true,
        implementation: (epochSeconds: number) => nullIsh(epochSeconds) ? null : new Date(epochSeconds * 1000),
    },
    {
        name: 'make_date',
        args: [DataType.integer, DataType.integer, DataType.integer],
        returns: DataType.date,
        implementation: (year: number, month: number, day: number) => {
            const ret = fromParts({ year, month: month - 1, day });
            if (!ret) {
                throw new QueryError(`date field value out of range: ${year}-${month}-${day}`);
            }
            return ret;
        },
    },
    ...[DataType.timestamp, DataType.timestamptz, DataType.date].map<FunctionDefinition>(arg => ({
        name: 'to_char',
        args: [arg, DataType.text],
        returns: DataType.text,
        implementation: dateToChar,
    })),
    {
        name: 'make_timestamp',
        args: [DataType.integer, DataType.integer, DataType.integer, DataType.integer, DataType.integer, DataType.float],
        returns: DataType.timestamp,
        implementation: (year: number, month: number, day: number, hour: number, min: number, sec: number) => {
            const whole = Math.floor(sec);
            const ms = Math.round((sec - whole) * 1000);
            const ret = fromParts({ year, month: month - 1, day, hour, minute: min, second: whole, millisecond: ms });
            if (!ret) {
                throw new QueryError(`date/time field value out of range`);
            }
            return ret;
        },
    },
    {
        name: 'make_time',
        args: [DataType.integer, DataType.integer, DataType.float],
        returns: DataType.time,
        implementation: (hour: number, min: number, sec: number) => {
            const pad = (n: number, w = 2) => String(n).padStart(w, '0');
            const whole = Math.floor(sec);
            return `${pad(hour)}:${pad(min)}:${pad(whole)}`;
        },
    },
    {
        name: 'to_char',
        args: [DataType.float, DataType.text],
        returns: DataType.text,
        implementation: numberToChar,
    },
    {
        name: 'to_char',
        args: [DataType.interval, DataType.text],
        returns: DataType.text,
        implementation: intervalToChar,
    },
    ...[DataType.timestamp, DataType.timestamptz].flatMap<FunctionDefinition>(arg => [{
        name: 'age',
        args: [arg, arg],
        returns: DataType.interval,
        implementation: (later: Date, earlier: Date) => dateAge(later, earlier),
    }, {
        name: 'age',
        args: [arg],
        returns: DataType.interval,
        impure: true,
        implementation: (of: Date) => dateAge(startOf(new Date(), 'day'), of),
    }]),
    {
        name: 'justify_interval',
        args: [DataType.interval],
        returns: DataType.interval,
        implementation: (v: Interval) => justifyInterval(v),
    },
    {
        name: 'justify_hours',
        args: [DataType.interval],
        returns: DataType.interval,
        implementation: (v: Interval) => justifyInterval(v, 'hours'),
    },
    {
        name: 'justify_days',
        args: [DataType.interval],
        returns: DataType.interval,
        implementation: (v: Interval) => justifyInterval(v, 'days'),
    },
];

/** Builds a pg-style symbolic interval, keeping only non-zero fields (like pg does) */
function toInterval(totalMonths: number, days: number, seconds: number): Interval {
    const ret: Interval = {};
    const years = Math.trunc(totalMonths / 12);
    const months = totalMonths % 12;
    const hours = Math.trunc(seconds / 3600);
    const minutes = Math.trunc(seconds % 3600 / 60);
    const secs = Math.trunc(seconds % 60);
    const milliseconds = Math.round((seconds - Math.trunc(seconds)) * 1000);
    if (years) ret.years = years;
    if (months) ret.months = months;
    if (days) ret.days = days;
    if (hours) ret.hours = hours;
    if (minutes) ret.minutes = minutes;
    if (secs) ret.seconds = secs;
    if (milliseconds) ret.milliseconds = milliseconds;
    return ret;
}

/** pg age(): symbolic calendar difference (years/months/days/time, not a duration).
 * Fields are subtracted independently, negative fields borrowing from the next larger
 * one - days borrow the length of the *earlier* date's month, like pg does. */
function dateAge(later: Date, earlier: Date): Interval {
    if (later < earlier) {
        const inv = dateAge(earlier, later);
        return Object.fromEntries(Object.entries(inv).map(([k, v]) => [k, -v!])) as Interval;
    }
    const a = utc(later);
    const b = utc(earlier);
    const daySeconds = (m: typeof a) => m.hours() * 3600
        + m.minutes() * 60
        + m.seconds()
        + m.milliseconds() / 1000;
    let seconds = daySeconds(a) - daySeconds(b);
    let days = a.date() - b.date();
    let months = a.month() - b.month();
    let years = a.year() - b.year();
    if (seconds < 0) {
        seconds += 86400;
        days--;
    }
    const earlierMonthDays = utc(fromParts({ year: b.year(), month: b.month(), day: 1 })!).daysInMonth();
    while (days < 0) {
        days += earlierMonthDays;
        months--;
    }
    if (months < 0) {
        months += 12;
        years--;
    }
    return toInterval(years * 12 + months, days, seconds);
}

function justifyInterval(v: Interval, only?: 'hours' | 'days'): Interval {
    let months = (v.years ?? 0) * 12 + (v.months ?? 0);
    let days = v.days ?? 0;
    let seconds = (v.hours ?? 0) * 3600
        + (v.minutes ?? 0) * 60
        + (v.seconds ?? 0)
        + (v.milliseconds ?? 0) / 1000;
    if (only !== 'days') {
        // move whole 24h days out of the time part
        days += Math.trunc(seconds / 86400);
        seconds = seconds % 86400;
    }
    if (only !== 'hours') {
        // move whole 30-day months out of the days part
        months += Math.trunc(days / 30);
        days = days % 30;
    }
    if (!only) {
        // borrow so all components carry the same sign
        if (months > 0 && (days < 0 || days === 0 && seconds < 0)) {
            months--;
            days += 30;
        } else if (months < 0 && (days > 0 || days === 0 && seconds > 0)) {
            months++;
            days -= 30;
        }
        if (days > 0 && seconds < 0) {
            days--;
            seconds += 86400;
        } else if (days < 0 && seconds > 0) {
            days++;
            seconds -= 86400;
        }
    }
    return toInterval(months, days, seconds);
}

const dateTruncUnits: { [field: string]: 'year' | 'quarter' | 'month' | 'isoWeek' | 'day' | 'hour' | 'minute' | 'second' | 'millisecond' } = {
    millennium: 'year',
    century: 'year',
    decade: 'year',
    year: 'year',
    quarter: 'quarter',
    month: 'month',
    week: 'isoWeek', // pg weeks are ISO weeks (start monday)
    day: 'day',
    hour: 'hour',
    minute: 'minute',
    second: 'second',
    milliseconds: 'millisecond',
};

function datePart(field: string, val: Date): number {
    const m = utc(val);
    switch (field?.toLowerCase()) {
        case 'millennium': return Math.ceil(m.year() / 1000);
        case 'century': return Math.ceil(m.year() / 100);
        case 'decade': return Math.floor(m.year() / 10);
        case 'year': return m.year();
        case 'quarter': return m.quarter();
        case 'month': return m.month() + 1;
        case 'day': return m.date();
        case 'hour': return m.hour();
        case 'minute': return m.minute();
        case 'second': return m.second() + m.millisecond() / 1000;
        case 'milliseconds': return m.second() * 1000 + m.millisecond();
        case 'dow': return m.day();
        case 'isodow': return m.isoWeekday();
        case 'doy': return m.dayOfYear();
        case 'week': return m.isoWeek();
        case 'isoyear': return m.isoWeekYear();
        case 'epoch': return m.valueOf() / 1000;
        default:
            throw new QueryError(`unit "${field}" not supported for type timestamp`);
    }
}