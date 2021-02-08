import { IValue, _IType, _ISelection, _ISchema, _IDb, _Transaction } from './interfaces-private';
import { Types, ArrayType } from './datatypes';
import { QueryError, NotSupported, nil } from './interfaces';
import { Evaluator } from './evaluator';
import hash from 'object-hash';
import { parseArrayLiteral } from 'pgsql-ast-parser';
import { nullIsh } from './utils';


export function buildCall(schema: _ISchema, name: string, args: IValue[]) {
    let type: _IType | nil = null;
    let get: (...args: any[]) => any;

    let impure = false;
    let acceptNulls = false;

    // put your ugly hack here 😶 🏴‍☠️ ...
    switch (name) {
        case 'any':
            return buildAnyCall(schema, args);
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
                throw new NotSupported(name + ' is not supported');
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
                throw new NotSupported(name + ' is not supported');
            };
            break;
        case 'coalesce':
            acceptNulls = true;
            args = args.map(x => x.convert(args[0].type));
            type = args[0].type;
            get = (...args: any[]) => args.find(x => !nullIsh(x));
            break;
        default:
            // try to find a matching custom function overloads
            acceptNulls = true;
            for (const o of schema.getFunctions(name, args.length)) {
                let ok = true;
                for (let i = 0; i < args.length; i++) {
                    const t = o.args[i]?.type ?? o.argsVariadic;
                    // calling 'out' arguments not supported
                    if (!t || !args[i].canConvert(t) || o.args[i]?.mode === 'out') {
                        ok = false;
                        break;
                    }
                }

                if (ok) {
                    args = args.map((x, i) => x.convert(o.args[i]?.type ?? o.argsVariadic));
                    type = o.returns;
                    get = o.implementation;
                    impure = !!o.impure;
                    break;
                }
            }


    }
    if (!get!) {
        throw new QueryError({
            error: `function ${name}(${args.map(() => 'unknown').join(',')}) does not exist`,
            hint: `🔨 Please note that pg-mem implements very few native functions.

            👉 You can specify the functions you would like to use via "db.public.registerFunction(...)"`
        })
    }
    return new Evaluator(
        schema
        , type ?? Types.null
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


function buildAnyCall(schema: _ISchema, args: IValue[]) {
    if (args.length !== 1) {
        throw new QueryError('ANY() expects 1 argument, given ' + args.length);
    }
    const array = args[0];

    // == if ANY(select something) ... get the element type
    if (array.type instanceof ArrayType) {
        return new Evaluator(
            schema
            , array.type.of
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
        schema
        , Types.text()
        , null
        , hash({ any: array.hash })
        , args
        , arrayValue
        , { isAny: true } // <== isAny !
    );
}