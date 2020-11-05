import { IValue, _IType, _ISelection } from './interfaces-private.ts';
import { Types, ArrayType } from './datatypes.ts';
import { QueryError, DataType, NotSupported } from './interfaces.ts';
import { Evaluator } from './valuetypes.ts';
import hash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';
import moment from 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts';
import { parseArrayLiteral } from './parser/parser.ts';
import { nullIsh } from './utils.ts';

export function buildCall(name: string, args: IValue[]) {
    let type: _IType;
    let get: (...args: any[]) => any;

    name = name.toLowerCase();
    let unpure = false;
    let acceptNulls = false;
    switch (name) {
        case 'lower':
        case 'upper':
            if (args.length !== 1) {
                throw new QueryError(name + ' expects one argument');
            }
            args = args.map(x => x.convert(DataType.text));
            type = args[0].type;
            if (name === 'lower') {
                get = (x: string) => x?.toLowerCase();
            } else {
                get = (x: string) => x?.toUpperCase();
            }
            break;
        case 'concat':
            acceptNulls = true;
            args = args.map(x => x.convert(DataType.text));
            type = Types.text();
            get = (...x: string[]) => x.join('');
            break;
        case 'to_date':
            if (args.length !== 2) {
                throw new QueryError('to_date expects 2 arguments, given ' + args.length);
            }
            args = args.map(x => x.convert(DataType.text))
            get = (data, format) => {
                if ((data ?? null) === null || (format ?? null) === null) {
                    return null; // if one argument is null => null
                }
                const ret = moment.utc(data, format);
                if (!ret.isValid()) {
                    throw new QueryError(`The text '${data}' does not match the date format ${format}`);
                }
                return ret.toDate();
            };
            type = Types.date;
            break;
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
        case 'now':
            if (args.length) {
                throw new QueryError('now expects no arguments, given ' + args.length);
            }
            type = Types.timestamp;
            get = () => new Date();
            unpure = true;
            break;
        case 'coalesce':
            acceptNulls = true;
            args = args.map(x => x.convert(args[0].type));
            type = args[0].type;
            get = (...args: any[]) => args.find(x => !nullIsh(x));
            break;
        default:
            throw new NotSupported('Unsupported function: ' + name);
    }
    return new Evaluator(
        type
        , null
        , `${name}(${args.map(x => x.sql).join(', ')})`
        , hash({ call: name, args: args.map(x => x.hash) })
        , args
        , (raw, t) => {
            const argRaw = args.map(x => x.get(raw, t));
            if (!acceptNulls && argRaw.some(nullIsh)) {
                return null;
            }
            return get(...argRaw);
        }, unpure ? { unpure } : undefined);
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
            , `ANY(${array.sql})`
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
        , `ANY(${array.sql})`
        , hash({ any: array.hash })
        , args
        , arrayValue
        , { isAny: true } // <== isAny !
    );
}