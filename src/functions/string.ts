import { DataType, FunctionDefinition, QueryError } from '../interfaces-private';
import { md5 } from '../utils/md5';
import { Types } from '../datatypes';
import { nullIsh } from '../utils';

function jsRegexFlags(flags: string): string {
    let f = '';
    if (flags.includes('i')) { f += 'i'; }
    return f;
}

/** regexp_matches: one row per match, each a text[] of capture groups (or whole match) */
function regexpMatches(str: string, pattern: string, flags: string): string[][] | null {
    if (nullIsh(str) || nullIsh(pattern)) { return null; }
    const global = flags.includes('g');
    const rx = new RegExp(pattern, 'g' + jsRegexFlags(flags));
    const rows: (string | null)[][] = [];
    let m: RegExpExecArray | null;
    while ((m = rx.exec(str)) !== null) {
        const groups = m.length > 1 ? m.slice(1) : [m[0]];
        rows.push(groups.map(g => g === undefined ? null : g));
        if (!global) { break; }
        if (m.index === rx.lastIndex) { rx.lastIndex++; } // avoid looping on empty match
    }
    return rows as string[][];
}

function regexpSplit(str: string, pattern: string, flags: string): string[] | null {
    if (nullIsh(str) || nullIsh(pattern)) { return null; }
    return str.split(new RegExp(pattern, jsRegexFlags(flags)));
}

// Unless allowNullArguments is set, null arguments short-circuit to a null result
// before the implementation is called, so implementations can assume non-null args.

function chars(x: string): string[] {
    return [...x];
}

// pg substr(): 1-based, out-of-range start/length are clamped, negative length throws
function pgSubstr(str: string, from: number, len?: number): string {
    const cs = chars(str);
    if (len !== undefined && len < 0) {
        throw new QueryError('negative substring length not allowed');
    }
    const start = Math.max(from, 1);
    const end = len === undefined ? cs.length + 1 : Math.max(from + len, 1);
    if (end <= start) {
        return '';
    }
    return cs.slice(start - 1, end - 1).join('');
}

function trimChars(x: string, toRemove: string, left: boolean, right: boolean): string {
    const set = new Set(chars(toRemove));
    let s = 0;
    let e = x.length;
    if (left) {
        while (s < e && set.has(x[s])) {
            s++;
        }
    }
    if (right) {
        while (e > s && set.has(x[e - 1])) {
            e--;
        }
    }
    return x.slice(s, e);
}

// pg lpad/rpad: truncates when target length is shorter than the input
function pad(str: string, len: number, fill: string, left: boolean): string {
    const cs = chars(str);
    if (len <= cs.length) {
        return cs.slice(0, Math.max(len, 0)).join('');
    }
    if (!fill) {
        return str;
    }
    const fillChars = chars(fill);
    let padding = '';
    for (let i = 0; i < len - cs.length; i++) {
        padding += fillChars[i % fillChars.length];
    }
    return left ? padding + str : str + padding;
}

function pgSplitPart(str: string, delimiter: string, field: number): string {
    if (field === 0) {
        throw new QueryError('field position must not be zero');
    }
    const fields = delimiter ? str.split(delimiter) : [str];
    const idx = field > 0 ? field - 1 : fields.length + field;
    return fields[idx] ?? '';
}

// pg regexp backrefs (\1..\9, \&) → js ($1..$9, $&)
function pgRegexpReplace(str: string, pattern: string, replacement: string, flags = ''): string {
    let jsFlags = '';
    if (flags.includes('g')) {
        jsFlags += 'g';
    }
    if (flags.includes('i')) {
        jsFlags += 'i';
    }
    const rep = replacement
        .replace(/\$/g, '$$$$')
        .replace(/\\(\d)/g, '$$$1')
        .replace(/\\&/g, '$$&');
    return str.replace(new RegExp(pattern, jsFlags), rep);
}

function quoteIdent(x: string): string {
    return /^[a-z_][a-z0-9_$]*$/.test(x) ? x : `"${x.replace(/"/g, '""')}"`;
}

export const stringFunctions: FunctionDefinition[] = [
    {
        name: 'lower',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => x?.toLowerCase(),
    },
    {
        name: 'upper',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => x?.toUpperCase(),
    },
    {
        name: 'concat',
        args: [DataType.text],
        argsVariadic: DataType.text,
        returns: DataType.text,
        allowNullArguments: true,
        implementation: (...x: string[]) => x?.join(''),
    },
    {
        name: 'concat_ws',
        args: [DataType.text],
        argsVariadic: DataType.text,
        returns: DataType.text,
        allowNullArguments: true,
        implementation: (separator: string, ...x: string[]) => x?.join(separator),
    },
    ...['length', 'char_length', 'character_length'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.text],
        returns: DataType.integer,
        implementation: (x: string) => chars(x).length,
    })),
    {
        name: 'octet_length',
        args: [DataType.text],
        returns: DataType.integer,
        implementation: (x: string) => new TextEncoder().encode(x).length,
    },
    {
        name: 'substr',
        args: [DataType.text, DataType.integer],
        returns: DataType.text,
        implementation: (x: string, from: number) => pgSubstr(x, from),
    },
    {
        name: 'substr',
        args: [DataType.text, DataType.integer, DataType.integer],
        returns: DataType.text,
        implementation: pgSubstr,
    },
    {
        name: 'replace',
        args: [DataType.text, DataType.text, DataType.text],
        returns: DataType.text,
        implementation: (x: string, from: string, to: string) => from ? x.split(from).join(to) : x,
    },
    {
        name: 'trim',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => trimChars(x, ' ', true, true),
    },
    {
        name: 'btrim',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => trimChars(x, ' ', true, true),
    },
    {
        name: 'btrim',
        args: [DataType.text, DataType.text],
        returns: DataType.text,
        implementation: (x: string, rm: string) => trimChars(x, rm, true, true),
    },
    {
        name: 'ltrim',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => trimChars(x, ' ', true, false),
    },
    {
        name: 'ltrim',
        args: [DataType.text, DataType.text],
        returns: DataType.text,
        implementation: (x: string, rm: string) => trimChars(x, rm, true, false),
    },
    {
        name: 'rtrim',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => trimChars(x, ' ', false, true),
    },
    {
        name: 'rtrim',
        args: [DataType.text, DataType.text],
        returns: DataType.text,
        implementation: (x: string, rm: string) => trimChars(x, rm, false, true),
    },
    {
        name: 'lpad',
        args: [DataType.text, DataType.integer],
        returns: DataType.text,
        implementation: (x: string, len: number) => pad(x, len, ' ', true),
    },
    {
        name: 'lpad',
        args: [DataType.text, DataType.integer, DataType.text],
        returns: DataType.text,
        implementation: (x: string, len: number, fill: string) => pad(x, len, fill, true),
    },
    {
        name: 'rpad',
        args: [DataType.text, DataType.integer],
        returns: DataType.text,
        implementation: (x: string, len: number) => pad(x, len, ' ', false),
    },
    {
        name: 'rpad',
        args: [DataType.text, DataType.integer, DataType.text],
        returns: DataType.text,
        implementation: (x: string, len: number, fill: string) => pad(x, len, fill, false),
    },
    {
        name: 'split_part',
        args: [DataType.text, DataType.text, DataType.integer],
        returns: DataType.text,
        implementation: pgSplitPart,
    },
    {
        name: 'strpos',
        args: [DataType.text, DataType.text],
        returns: DataType.integer,
        implementation: (x: string, search: string) => x.indexOf(search) + 1,
    },
    {
        name: 'initcap',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => x.toLowerCase()
            .replace(/[\p{L}\p{N}]+/gu, w => w[0].toUpperCase() + w.slice(1)),
    },
    {
        name: 'reverse',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => chars(x).reverse().join(''),
    },
    {
        name: 'left',
        args: [DataType.text, DataType.integer],
        returns: DataType.text,
        implementation: (x: string, n: number) => {
            const cs = chars(x);
            return (n >= 0 ? cs.slice(0, n) : cs.slice(0, Math.max(cs.length + n, 0))).join('');
        },
    },
    {
        name: 'right',
        args: [DataType.text, DataType.integer],
        returns: DataType.text,
        implementation: (x: string, n: number) => {
            const cs = chars(x);
            return (n >= 0 ? cs.slice(Math.max(cs.length - n, 0)) : cs.slice(-n)).join('');
        },
    },
    {
        name: 'repeat',
        args: [DataType.text, DataType.integer],
        returns: DataType.text,
        implementation: (x: string, n: number) => n > 0 ? x.repeat(n) : '',
    },
    {
        name: 'md5',
        args: [DataType.text],
        returns: DataType.text,
        implementation: md5,
    },
    {
        name: 'ascii',
        args: [DataType.text],
        returns: DataType.integer,
        implementation: (x: string) => x.length ? x.codePointAt(0) : 0,
    },
    {
        name: 'chr',
        args: [DataType.integer],
        returns: DataType.text,
        implementation: (n: number) => {
            if (n === 0) {
                throw new QueryError('null character not permitted');
            }
            return String.fromCodePoint(n);
        },
    },
    {
        name: 'translate',
        args: [DataType.text, DataType.text, DataType.text],
        returns: DataType.text,
        implementation: (x: string, from: string, to: string) => {
            const map = new Map<string, string>();
            const toChars = chars(to);
            chars(from).forEach((c, i) => {
                if (!map.has(c)) {
                    map.set(c, toChars[i] ?? '');
                }
            });
            return chars(x).map(c => map.get(c) ?? c).join('');
        },
    },
    {
        name: 'starts_with',
        args: [DataType.text, DataType.text],
        returns: DataType.bool,
        implementation: (x: string, prefix: string) => x.startsWith(prefix),
    },
    {
        name: 'regexp_replace',
        args: [DataType.text, DataType.text, DataType.text],
        returns: DataType.text,
        implementation: (x: string, pattern: string, replacement: string) =>
            pgRegexpReplace(x, pattern, replacement),
    },
    {
        name: 'regexp_replace',
        args: [DataType.text, DataType.text, DataType.text, DataType.text],
        returns: DataType.text,
        implementation: pgRegexpReplace,
    },
    {
        // one row per match; each row is a text[] of the capture groups
        // (or the whole match when the pattern has no groups)
        name: 'regexp_matches',
        args: [DataType.text, DataType.text],
        returns: Types.text().asArray().asArray(),
        setReturning: true,
        allowNullArguments: true,
        implementation: (str: string, pattern: string) => regexpMatches(str, pattern, ''),
    },
    {
        name: 'regexp_matches',
        args: [DataType.text, DataType.text, DataType.text],
        returns: Types.text().asArray().asArray(),
        setReturning: true,
        allowNullArguments: true,
        implementation: (str: string, pattern: string, flags: string) => regexpMatches(str, pattern, flags),
    },
    {
        name: 'regexp_split_to_array',
        args: [DataType.text, DataType.text],
        returns: Types.text().asArray(),
        allowNullArguments: true,
        implementation: (str: string, pattern: string) => regexpSplit(str, pattern, ''),
    },
    {
        name: 'regexp_split_to_array',
        args: [DataType.text, DataType.text, DataType.text],
        returns: Types.text().asArray(),
        allowNullArguments: true,
        implementation: (str: string, pattern: string, flags: string) => regexpSplit(str, pattern, flags),
    },
    {
        name: 'regexp_split_to_table',
        args: [DataType.text, DataType.text],
        returns: Types.text().asArray(),
        setReturning: true,
        allowNullArguments: true,
        implementation: (str: string, pattern: string) => regexpSplit(str, pattern, ''),
    },
    {
        name: 'regexp_split_to_table',
        args: [DataType.text, DataType.text, DataType.text],
        returns: Types.text().asArray(),
        setReturning: true,
        allowNullArguments: true,
        implementation: (str: string, pattern: string, flags: string) => regexpSplit(str, pattern, flags),
    },
    {
        name: 'format',
        args: [DataType.text],
        argsVariadic: DataType.text,
        returns: DataType.text,
        allowNullArguments: true,
        implementation: (fmt: string, ...args: (string | null)[]) => {
            if (fmt === null || fmt === undefined) {
                return null;
            }
            let next = 0;
            return fmt.replace(/%(?:(\d+)\$)?([sIL%])/g, (_, pos: string | undefined, spec: string) => {
                if (spec === '%') {
                    return '%';
                }
                const arg = args[pos ? +pos - 1 : next++];
                switch (spec) {
                    case 's':
                        return arg === null || arg === undefined ? '' : String(arg);
                    case 'I':
                        if (arg === null || arg === undefined) {
                            throw new QueryError('null values cannot be formatted as an SQL identifier');
                        }
                        return quoteIdent(String(arg));
                    case 'L':
                        return arg === null || arg === undefined
                            ? 'NULL'
                            : `'${String(arg).replace(/'/g, `''`)}'`;
                    default:
                        return '';
                }
            });
        },
    },
]
