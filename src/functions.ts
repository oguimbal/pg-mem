import { IValue, _IType, _ISelection } from './interfaces-private';
import { Types } from './datatypes';
import { QueryError, DataType } from './interfaces';
import { NotSupported } from './utils';
import { Evaluator } from './valuetypes';
import hash from 'object-hash';

export function buildCall(name: string, args: IValue[]) {
    let type: _IType;
    let get: (...args: any[]) => any;
    const sels = [...new Set(args.map(x => x.selection).filter(x => !!x))];
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
        default:
            throw new NotSupported('Unsupported function: ' + name);
    }
    return new Evaluator(
        type
        , null
        , `${name}(${args.map(x => x.sql).join(', ')})`
        , hash({ call: name, args: args.map(x => x.hash) })
        , sels.length === 1 ? sels[0] : null
        , raw => {
            const argRaw = args.map(x => x.get(raw));
            return get(...argRaw);
        });
}