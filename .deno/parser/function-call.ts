import { IValue, _IType, _ISelection, _ISchema, _IDb, _Transaction } from '../interfaces-private.ts';
import { Types, ArrayType } from '../datatypes/index.ts';
import { QueryError, NotSupported, nil } from '../interfaces.ts';
import { Evaluator } from '../evaluator.ts';
import hash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';
import { parseArrayLiteral, QName } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { asSingleQName, nullIsh, qnameToStr } from '../utils.ts';
import { buildCtx } from './context.ts';


export function buildCall(name: string | QName, args: IValue[]): IValue {
    let type: _IType | nil = null;
    let get: (...args: any[]) => any;

    let impure = false;
    let acceptNulls = false;
    const { schema } = buildCtx();

    // put your ugly hack here 😶 🏴‍☠️ ...
    switch (asSingleQName(name)) {
        case 'any':
            return buildAnyCall(args);
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
        case 'unnest':
            if (args.length !== 1) {
                throw new QueryError('unnest expects 1 arguments, given ' + args.length);
            }
            const utype = args[0].type;
            if (!(utype instanceof ArrayType)) {
                throw new QueryError('unnest expects enumerable argument ' + utype.primary);
            }
            type = utype.of;
            get = () => {
                throw new NotSupported(qnameToStr(name) + ' is not supported');
            };
            break;
        case 'coalesce':
            acceptNulls = true;
            if (!args.length) {
                throw new QueryError('coalesce expects at least 1 argument');
            }
            type = args.reduce<_IType>((a, b) => {
                if (a === b.type) {
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
    return new Evaluator(
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
}


function buildAnyCall(args: IValue[]) {
    if (args.length !== 1) {
        throw new QueryError('ANY() expects 1 argument, given ' + args.length);
    }
    const array = args[0];

    // == if ANY(select something) ... get the element type
    if (array.type instanceof ArrayType) {
        return new Evaluator(
            array.type.of
            , null
            , hash({ any: array.hash })
            , args
            , (raw, t) => {
                return array.get(raw, t);
            }
            , { isAny: true } // <== isAny !
        );
    }

    // == if ANY('{elements}') ... will be an array of text => keep text

    if (array.type !== Types.text() || !array.isConstantLiteral) {
        throw new QueryError('ANY() expects either a selection, or an array literal');
    }
    // parse ANY() array literal
    const arrayValue = parseArrayLiteral(array.get());
    return new Evaluator(
        Types.text()
        , null
        , hash({ any: array.hash })
        , args
        , arrayValue
        , { isAny: true } // <== isAny !
    );
}