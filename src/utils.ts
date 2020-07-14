import moment from 'moment';

export class NotSupported extends Error {
    constructor(what?: string) {
        super('Not supported' + (what ? ': ' + what : ''));
    }
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
