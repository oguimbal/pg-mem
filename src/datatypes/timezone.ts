import { QueryError } from '../interfaces';
import { GLOBAL_VARS } from '../interfaces-private';
import { executionCtx } from '../utils';

/** The session TimeZone GUC (defaults to UTC). */
export function sessionTimezone(): string {
    try {
        return executionCtx().transaction.getMap(GLOBAL_VARS).get('timezone') ?? 'UTC';
    } catch {
        return 'UTC';
    }
}

// Timezone support without a bundled tz database: named zones (IANA) resolve through
// the JS runtime's own Intl tz data; fixed offsets are parsed directly.

const fixedOffset = /^(?:UTC|GMT)?\s*([+-])(\d{1,2})(?::?(\d{2}))?$/i;

const intlCache = new Map<string, Intl.DateTimeFormat>();
function formatterFor(zone: string): Intl.DateTimeFormat {
    let f = intlCache.get(zone);
    if (!f) {
        try {
            f = new Intl.DateTimeFormat('en-US', {
                timeZone: zone,
                hour12: false,
                year: 'numeric', month: '2-digit', day: '2-digit',
                hour: '2-digit', minute: '2-digit', second: '2-digit',
            });
        } catch {
            throw new QueryError(`time zone "${zone}" not recognized`);
        }
        intlCache.set(zone, f);
    }
    return f;
}

/**
 * Offset of `zone` from UTC in minutes (east of UTC positive) at the given instant.
 * Accepts "UTC"/"GMT", fixed offsets ("+05", "UTC-3", "+05:30") and IANA names.
 */
export function zoneOffsetMinutes(zone: string, instant: Date): number {
    if (/^(UTC|GMT)$/i.test(zone.trim())) {
        return 0;
    }
    const m = zone.trim().match(fixedOffset);
    if (m) {
        const sign = m[1] === '-' ? -1 : 1;
        return sign * (parseInt(m[2], 10) * 60 + (m[3] ? parseInt(m[3], 10) : 0));
    }
    // named zone: derive the offset by formatting the instant in that zone and
    // comparing the wall-clock it reports against the same instant in UTC
    const parts = formatterFor(zone).formatToParts(instant);
    const get = (t: string) => +parts.find(p => p.type === t)!.value;
    const asUtc = Date.UTC(get('year'), get('month') - 1, get('day'), get('hour') % 24, get('minute'), get('second'));
    return Math.round((asUtc - instant.getTime()) / 60000);
}

/** `timestamp AT TIME ZONE zone`: interpret the wall-clock as local time in `zone`,
 * producing the UTC instant (a timestamptz). */
export function timestampAtZone(wall: Date, zone: string): Date {
    // wall is stored as if UTC; shift back by the zone offset to get the true instant
    const offset = zoneOffsetMinutes(zone, wall);
    return new Date(wall.getTime() - offset * 60000);
}

/** `timestamptz AT TIME ZONE zone`: render the instant as wall-clock time in `zone`
 * (a plain timestamp). */
export function instantToZoneWall(instant: Date, zone: string): Date {
    const offset = zoneOffsetMinutes(zone, instant);
    return new Date(instant.getTime() + offset * 60000);
}

function pad(n: number, len = 2): string {
    return String(Math.abs(n)).padStart(len, '0');
}

/** Render a UTC wall-clock Date as a postgres timestamp string (no zone). */
export function renderTimestamp(d: Date): string {
    const base = `${pad(d.getUTCFullYear(), 4)}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())}`
        + ` ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
    const ms = d.getUTCMilliseconds();
    return ms ? `${base}.${pad(ms, 3)}` : base;
}

/** Render an instant as a postgres timestamptz string in the given session zone. */
export function renderTimestamptz(instant: Date, sessionZone: string): string {
    const offset = zoneOffsetMinutes(sessionZone, instant);
    const local = new Date(instant.getTime() + offset * 60000);
    const sign = offset < 0 ? '-' : '+';
    const hh = pad(Math.floor(Math.abs(offset) / 60));
    const mm = Math.abs(offset) % 60;
    const tz = mm ? `${sign}${hh}:${pad(mm)}` : `${sign}${hh}`;
    return renderTimestamp(local) + tz;
}
