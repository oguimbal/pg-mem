import { IValue, _IType, _ISelection } from './interfaces-private';
import { Types, singleSelection } from './datatypes';
import { QueryError, DataType, NotSupported } from './interfaces';
import { Evaluator } from './valuetypes';
import hash from 'object-hash';
import moment from 'moment';

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