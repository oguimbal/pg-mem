import { IValue, _IType, _ISelection } from './interfaces-private';
import { Types, singleSelection, ArrayType } from './datatypes';
import { QueryError, DataType, NotSupported } from './interfaces';
import { Evaluator } from './valuetypes';
import hash from 'object-hash';
import moment from 'moment';
import { parseArrayLiteral } from './parser/parser';

export function buildCall(name: string, args: IValue[]) {
    let type: _IType;
    let get: (...args: any[]) => any;

    name = name.toLowerCase();
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
                const ret = moment(data, format);
                if (!ret.isValid()) {
                    throw new QueryError(`The text '${data}' does not match the date format ${format}`);
                }
                return ret.toDate();
            };
            type = Types.date;
            break;
        case 'any':
            return buildAnyCall(args);
        default:
            throw new NotSupported('Unsupported function: ' + name);
    }
    return new Evaluator(
        type
        , null
        , `${name}(${args.map(x => x.sql).join(', ')})`
        , hash({ call: name, args: args.map(x => x.hash) })
        , singleSelection(args)
        , raw => {
            const argRaw = args.map(x => x.get(raw));
            return get(...argRaw);
        });
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
            , singleSelection(args)
            , raw => {
                return array.get(raw);
            }
            , true // <== isAny !
        );
    }

    // == if ANY('{elements}') ... will be an array of text => keep text

    if (array.type !== Types.text() || !array.isConstantLiteral) {
        throw new QueryError('ANY() expects either a selection, or an array literal');
    }
    // parse ANY() array literal
    const arrayValue = parseArrayLiteral(array.get(null));
    return new Evaluator(
        Types.text()
        , null
        , `ANY(${array.sql})`
        , hash({ any: array.hash })
        , singleSelection(args)
        , arrayValue
        , true // <== isAny !
    );
}