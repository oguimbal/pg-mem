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


export function watchUse<T>(value: T): T & { check?(); } {
    if (!value || globalThis?.process?.env?.['NOCHECKFULLQUERYUSAGE']) {
        return value;
    }
    if (typeof value !== 'object') {
        throw new NotSupported();
    }
    const ret = {};
    const toUse = new Set<string>();
    const watchables: { check(); }[] = [];
    for (const [k, _v] of Object.entries(value)) {
        let v = _v;
        if (Array.isArray(v)) {
            const trans = [];
            for (const x of v) {
                if (typeof x === 'object' && x) {
                    const w = watchUse(x);
                    watchables.push(w);
                    trans.push(w);
                } else {
                    trans.push(x);
                }
            }
            v = trans;
        } else if (typeof v === 'object' && v) {
            v = watchUse(v);
            watchables.push(v);
        }
        if (v === null || v === undefined) {
            continue;
        }
        toUse.add(k);
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
            throw new NotSupported('query parts ' + [...toUse].join(', '));
        }
        for (const w of watchables) {
            w?.check();
        }
    }
    return ret as any;
}
