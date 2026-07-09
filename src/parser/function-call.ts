import { IValue, _IType, _ISelection, _ISchema, _IDb, _Transaction } from '../interfaces-private';
import { Types, ArrayType } from '../datatypes';
import { QueryError, NotSupported, nil, DataType } from '../interfaces';
import { Evaluator } from '../evaluator';
import hash from 'object-hash';
import { parseArrayLiteral, QName } from 'pgsql-ast-parser';
import { asSingleQName, nullIsh, qnameToStr } from '../utils';
import { buildCtx } from './context';
import { markSetReturning } from '../transforms/expand-srf';


export function buildCall(name: string | QName, args: IValue[]): IValue {
    let type: _IType | nil = null;
    let get: (...args: any[]) => any;

    let impure = false;
    let acceptNulls = false;
    let setReturning = false;
    const { schema } = buildCtx();

    // put your ugly hack here 😶 🏴‍☠️ ...
    switch (asSingleQName(name)) {
        case 'any':
            return buildAnyCall(args);
        case 'all':
            return buildAllCall(args);
        case 'current_schema':
            type = Types.text();
            get = () => 'public';
            break;
        // a set of functions that are calledby Tyopeorm, but we dont needto support them yet
        // since there is not result (function never actually called)
        case 'pg_get_constraintdef':
        case 'pg_get_expr':
            type = Types.text();
            get = () => {
                throw new NotSupported(qnameToStr(name) + ' is not supported');
            };
            break;
        case 'unnest': {
            if (args.length !== 1) {
                throw new QueryError('unnest expects 1 arguments, given ' + args.length);
            }
            const utype = args[0].type;
            if (!(utype instanceof ArrayType)) {
                throw new QueryError('unnest expects enumerable argument ' + utype.primary);
            }
            // yields the array as-is: set-expansion is handled by the FROM clause
            // or the select-list SRF expansion
            type = utype;
            setReturning = true;
            get = (arr: any[]) => arr;
            break;
        }
        // polymorphic json builders
        case 'to_jsonb':
        case 'to_json': {
            expectArgs(name, args, 1);
            type = Types.jsonb;
            acceptNulls = true;
            get = (v: any) => v instanceof Date ? v.toISOString() : v ?? null;
            break;
        }
        case 'json_build_object':
        case 'jsonb_build_object': {
            if (args.length % 2) {
                throw new QueryError('argument list must have even number of elements', '42601');
            }
            type = Types.jsonb;
            acceptNulls = true;
            get = (...kv: any[]) => {
                const ret: any = {};
                for (let i = 0; i < kv.length; i += 2) {
                    if (nullIsh(kv[i])) {
                        throw new QueryError(`argument ${i + 1}: key must not be null`, '22004');
                    }
                    ret[String(kv[i])] = kv[i + 1] ?? null;
                }
                return ret;
            };
            break;
        }
        case 'json_build_array':
        case 'jsonb_build_array': {
            type = Types.jsonb;
            acceptNulls = true;
            get = (...vals: any[]) => vals.map(v => v ?? null);
            break;
        }
        // polymorphic array functions: need the actual element type of their argument,
        // which FunctionDefinition cannot express (no "anyarray" pseudo-type yet)
        case 'array_length':
        case 'array_upper': {
            expectArgs(name, args, 2);
            requireArrayArg(name, args[0]);
            type = Types.integer;
            get = (arr: any[], dim: number) => dim === 1 && arr.length ? arr.length : null;
            break;
        }
        case 'array_lower': {
            expectArgs(name, args, 2);
            requireArrayArg(name, args[0]);
            type = Types.integer;
            get = (arr: any[], dim: number) => dim === 1 && arr.length ? 1 : null;
            break;
        }
        case 'cardinality': {
            expectArgs(name, args, 1);
            requireArrayArg(name, args[0]);
            type = Types.integer;
            get = (arr: any[]) => arr.length;
            break;
        }
        case 'generate_subscripts': {
            expectArgs(name, args, 2);
            requireArrayArg(name, args[0]);
            type = Types.integer.asArray();
            setReturning = true;
            // pg-mem arrays are one-dimensional: dim 1 yields 1..length, else nothing
            get = (arr: any[] | null, dim: number) =>
                dim === 1 && Array.isArray(arr) ? arr.map((_, i) => i + 1) : [];
            break;
        }
        case 'array_append': {
            expectArgs(name, args, 2);
            const appendTo = requireArrayArg(name, args[0]);
            args = [args[0], args[1].cast(appendTo.of)];
            type = appendTo;
            acceptNulls = true; // appending a null element is legit
            get = (arr: any[] | null, el: any) => nullIsh(arr) ? [el] : [...arr!, el];
            break;
        }
        case 'array_cat': {
            expectArgs(name, args, 2);
            const catTo = requireArrayArg(name, args[0]);
            args = [args[0], args[1].cast(catTo)];
            type = catTo;
            get = (a: any[], b: any[]) => [...a, ...b];
            break;
        }
        case 'array_position': {
            expectArgs(name, args, 2);
            const searched = requireArrayArg(name, args[0]);
            args = [args[0], args[1].cast(searched.of)];
            type = Types.integer;
            acceptNulls = true; // pg finds null elements
            get = (arr: any[] | null, el: any) => {
                if (nullIsh(arr)) {
                    return null;
                }
                const idx = arr!.findIndex(v => nullIsh(v) || nullIsh(el)
                    ? nullIsh(v) === nullIsh(el)
                    : searched.of.equals(v, el));
                return idx < 0 ? null : idx + 1;
            };
            break;
        }
        case 'array_remove': {
            expectArgs(name, args, 2);
            const remFrom = requireArrayArg(name, args[0]);
            args = [args[0], args[1].cast(remFrom.of)];
            type = remFrom;
            acceptNulls = true; // array_remove(arr, null) removes null elements
            get = (arr: any[] | null, el: any) => {
                if (nullIsh(arr)) { return null; }
                const en = nullIsh(el);
                return arr!.filter(v => {
                    const vn = nullIsh(v);
                    if (vn || en) { return !(vn && en); }
                    return !remFrom.of.equals(v, el);
                });
            };
            break;
        }
        case 'array_replace': {
            expectArgs(name, args, 3);
            const repIn = requireArrayArg(name, args[0]);
            args = [args[0], args[1].cast(repIn.of), args[2].cast(repIn.of)];
            type = repIn;
            acceptNulls = true;
            get = (arr: any[] | null, from: any, to: any) => {
                if (nullIsh(arr)) { return null; }
                const fn = nullIsh(from);
                return arr!.map(v => {
                    const vn = nullIsh(v);
                    const match = (vn || fn) ? (vn && fn) : repIn.of.equals(v, from);
                    return match ? to : v;
                });
            };
            break;
        }
        case 'row_to_json':
        case 'array_to_json': {
            expectArgs(name, args, [1, 2]);
            type = Types.json;
            acceptNulls = true;
            get = (v: any) => v ?? null;
            break;
        }
        case 'array_to_string': {
            expectArgs(name, args, [2, 3]);
            requireArrayArg(name, args[0]);
            type = Types.text();
            get = (arr: any[], sep: string, nullStr?: string) => arr
                .filter(v => !nullIsh(v) || nullStr !== undefined)
                .map(v => nullIsh(v) ? nullStr : String(v))
                .join(sep);
            break;
        }
        case 'nullif': {
            expectArgs(name, args, 2);
            const nullifType = args[0].type;
            args = [args[0], args[1].cast(nullifType)];
            type = nullifType;
            acceptNulls = true; // nullif(1, null) returns 1, not null
            get = (a: any, b: any) => {
                if (nullIsh(a)) {
                    return null;
                }
                return !nullIsh(b) && nullifType.equals(a, b) ? null : a;
            };
            break;
        }
        case 'coalesce':
            acceptNulls = true;
            if (!args.length) {
                throw new QueryError('coalesce expects at least 1 argument');
            }
            type = args.reduce<_IType>((a, b) => {
                if (a === b.type) {
                    return a;
                }
                // prefer implicit conversion (postgres common-type rules)
                if (a.canConvertImplicit(b.type)) {
                    return b.type;
                }
                if (b.type.canConvertImplicit(a)) {
                    return a;
                }
                // otherwise fall back to explicit casts, but a plain-text operand yields
                // to a concrete type (an untyped string literal coerces to it in postgres,
                // e.g. coalesce('2020-01-01', ts) resolves to timestamp, not text)
                if (a.primary === DataType.text && b.type.canCast(a)) {
                    return b.type;
                }
                if (b.type.primary === DataType.text && a.canCast(b.type)) {
                    return a;
                }
                if (b.type.canCast(a)) {
                    return a;
                }
                if (a.canCast(b.type)) {
                    return b.type;
                }
                throw new QueryError(`COALESCE types ${a.name} and ${b.type.name} cannot be matched`, '42804');
            }, args[0].type);
            args = args.map(x => x.cast(type!));
            get = (...args: any[]) => args.find(x => !nullIsh(x));
            break;
        default:
            // try to find a matching custom function overloads
            acceptNulls = true;
            const resolved = schema.resolveFunction(name, args);
            if (resolved) {
                args = args.map((x, i) => x.cast(resolved.args[i]?.type ?? resolved.argsVariadic));
                type = resolved.returns;
                get = resolved.implementation;
                impure = !!resolved.impure;
                acceptNulls = !!resolved.allowNullArguments;
                setReturning = !!resolved.setReturning;
            }
            break;

    }
    if (!get!) {
        throw new QueryError({
            error: `function ${qnameToStr(name)}(${args.map(a => a.type.name).join(',')}) does not exist`,
            hint: `🔨 Please note that pg-mem implements very few native functions.

            👉 You can specify the functions you would like to use via "db.public.registerFunction(...)"`
        })
    }
    const ret = new Evaluator(
        type ?? Types.null
        , null
        , hash({ call: name, args: args.map(x => x.hash) })
        , args
        , (raw, t) => {
            const argRaw = args.map(x => x.get(raw, t));
            if (!acceptNulls && argRaw.some(nullIsh)) {
                return null;
            }
            return get(...argRaw);
        }, impure ? { unpure: impure } : undefined);
    return setReturning ? markSetReturning(ret) : ret;
}


function expectArgs(name: string | QName, args: IValue[], count: number | [number, number]) {
    const [min, max] = typeof count === 'number' ? [count, count] : count;
    if (args.length < min || args.length > max) {
        throw new QueryError(`function ${qnameToStr(name)} expects ${min === max ? min : `${min} to ${max}`} argument(s), given ${args.length}`);
    }
}

function requireArrayArg(name: string | QName, arg: IValue): ArrayType {
    if (!(arg.type instanceof ArrayType)) {
        throw new QueryError(`function ${qnameToStr(name)} expects an array argument, given ${arg.type.name}`);
    }
    return arg.type;
}

function buildAnyCall(args: IValue[]) {
    return buildQuantifiedCall(args, 'any');
}

function buildAllCall(args: IValue[]) {
    return buildQuantifiedCall(args, 'all');
}

function buildQuantifiedCall(args: IValue[], kind: 'any' | 'all') {
    const label = kind.toUpperCase();
    if (args.length !== 1) {
        throw new QueryError(`${label}() expects 1 argument, given ` + args.length);
    }
    const array = args[0];
    const opts = kind === 'any' ? { isAny: true } : { isAll: true };

    // == if ANY/ALL(select something) ... get the element type
    if (array.type instanceof ArrayType) {
        return new Evaluator(
            array.type.of
            , null
            , hash({ [kind]: array.hash })
            , args
            , (raw, t) => {
                return array.get(raw, t);
            }
            , opts
        );
    }

    // == if ANY/ALL('{elements}') ... will be an array of text => keep text

    if (array.type !== Types.text() || !array.isConstantLiteral) {
        throw new QueryError(`${label}() expects either a selection, or an array literal`);
    }
    // parse the array literal
    const arrayValue = parseArrayLiteral(array.get());
    return new Evaluator(
        Types.text()
        , null
        , hash({ [kind]: array.hash })
        , args
        , arrayValue
        , opts
    );
}