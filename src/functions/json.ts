import { DataType, FunctionDefinition, QueryError } from '../interfaces-private';
import { Types } from '../datatypes';
import { nullIsh } from '../utils';
import { JSON_NIL } from '../execution/clean-results';

// pg-mem stores json/jsonb values as plain JS values, except json "null", which is
// the JSON_NIL sentinel (to tell it apart from the SQL NULL)

function isJsonNull(v: any): boolean {
    return nullIsh(v) || v === JSON_NIL;
}

export function jsonPathGet(value: any, path: string[]): any {
    let cur = value;
    for (const p of path) {
        if (isJsonNull(cur) || typeof cur !== 'object') {
            return null;
        }
        cur = Array.isArray(cur) ? cur[normalizeIndex(p, cur.length, false)] : cur[p];
        if (cur === undefined) {
            return null;
        }
    }
    return cur;
}

export function jsonAsText(v: any): string | null {
    if (isJsonNull(v)) {
        return null;
    }
    return typeof v === 'string' ? v : JSON.stringify(v);
}

function normalizeIndex(raw: string, len: number, clamp: boolean): number {
    const idx = +raw;
    if (!Number.isInteger(idx)) {
        return NaN;
    }
    const fromEnd = idx < 0 ? len + idx : idx;
    if (!clamp) {
        return fromEnd;
    }
    return Math.max(0, Math.min(fromEnd, len));
}

function jsonSet(target: any, path: string[], newVal: any, createMissing: boolean): any {
    if (!path.length) {
        return target;
    }
    if (nullIsh(target) || typeof target !== 'object') {
        throw new QueryError(`cannot set path in scalar`, '22023');
    }
    const [head, ...rest] = path;
    if (Array.isArray(target)) {
        const idx = normalizeIndex(head, target.length, false);
        if (isNaN(idx)) {
            throw new QueryError(`path element at position 1 is not an integer: "${head}"`, '22P02');
        }
        const ret = [...target];
        const at = Math.max(0, Math.min(idx, target.length));
        if (idx >= 0 && idx < target.length || rest.length) {
            ret[Math.min(Math.max(idx, 0), target.length - 1)] = rest.length
                ? jsonSet(target[Math.min(Math.max(idx, 0), target.length - 1)], rest, newVal, createMissing)
                : newVal;
        } else if (createMissing) {
            // out of range: pg appends/prepends
            if (idx < 0) {
                ret.unshift(newVal);
            } else {
                ret.push(newVal);
            }
        }
        return ret;
    }
    const has = head in target;
    if (!has && !createMissing) {
        return target;
    }
    return {
        ...target,
        [head]: rest.length
            ? jsonSet(has ? target[head] : {}, rest, newVal, createMissing)
            : newVal,
    };
}

function jsonTypeof(v: any): string | null {
    if (v === undefined) {
        return null;
    }
    if (v === null || v === JSON_NIL) {
        return 'null';
    }
    if (Array.isArray(v)) {
        return 'array';
    }
    switch (typeof v) {
        case 'object': return 'object';
        case 'string': return 'string';
        case 'number': return 'number';
        case 'boolean': return 'boolean';
        default: return null;
    }
}

function requireJsonArray(fn: string, v: any): any[] {
    if (!Array.isArray(v)) {
        throw new QueryError(`cannot get array length of a non-array`, '22023');
    }
    return v;
}

const eachRecord = Types.record([
    { name: 'key', type: Types.text() },
    { name: 'value', type: Types.jsonb },
]);
const eachTextRecord = Types.record([
    { name: 'key', type: Types.text() },
    { name: 'value', type: Types.text() },
]);

export const jsonFunctions: FunctionDefinition[] = [
    ...['jsonb_array_length', 'json_array_length'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.jsonb],
        returns: DataType.integer,
        implementation: (v: any) => requireJsonArray(name, v).length,
    })),
    {
        name: 'jsonb_set',
        args: [DataType.jsonb, Types.text().asArray(), DataType.jsonb],
        returns: DataType.jsonb,
        implementation: (target: any, path: string[], newVal: any) => jsonSet(target, path, newVal, true),
    },
    {
        name: 'jsonb_set',
        args: [DataType.jsonb, Types.text().asArray(), DataType.jsonb, DataType.bool],
        returns: DataType.jsonb,
        implementation: jsonSet,
    },
    ...['jsonb_extract_path', 'json_extract_path'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.jsonb],
        argsVariadic: DataType.text,
        returns: DataType.jsonb,
        implementation: (v: any, ...path: string[]) => jsonPathGet(v, path),
    })),
    ...['jsonb_extract_path_text', 'json_extract_path_text'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.jsonb],
        argsVariadic: DataType.text,
        returns: DataType.text,
        implementation: (v: any, ...path: string[]) => jsonAsText(jsonPathGet(v, path)),
    })),
    ...['jsonb_typeof', 'json_typeof'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.jsonb],
        returns: DataType.text,
        implementation: jsonTypeof,
    })),
    {
        name: 'jsonb_strip_nulls',
        args: [DataType.jsonb],
        returns: DataType.jsonb,
        implementation: function strip(v: any): any {
            if (Array.isArray(v)) {
                return v.map(x => strip(x));
            }
            if (v && typeof v === 'object') {
                return Object.fromEntries(Object.entries(v)
                    .filter(([, val]) => !isJsonNull(val))
                    .map(([k, val]) => [k, strip(val)]));
            }
            return v;
        },
    },
    // set-returning: one output row per element, in FROM position or a select list
    ...['jsonb_object_keys', 'json_object_keys'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.jsonb],
        returns: Types.text().asArray(),
        setReturning: true,
        implementation: (v: any) => {
            if (nullIsh(v) || typeof v !== 'object' || Array.isArray(v)) {
                throw new QueryError('cannot call jsonb_object_keys on a non-object', '22023');
            }
            return Object.keys(v);
        },
    })),
    {
        name: 'jsonb_each',
        args: [DataType.jsonb],
        returns: eachRecord.asArray(),
        setReturning: true,
        implementation: (v: any) => Object.entries(v ?? {}).map(([key, value]) => ({ key, value })),
    },
    {
        name: 'jsonb_each_text',
        args: [DataType.jsonb],
        returns: eachTextRecord.asArray(),
        setReturning: true,
        implementation: (v: any) => Object.entries(v ?? {}).map(([key, value]) => ({ key, value: jsonAsText(value) })),
    },
    ...['jsonb_array_elements', 'json_array_elements'].map<FunctionDefinition>(name => ({
        name,
        args: [DataType.jsonb],
        returns: Types.jsonb.asArray(),
        setReturning: true,
        implementation: (v: any) => requireJsonArray(name, v),
    })),
];
