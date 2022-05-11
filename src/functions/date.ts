import { FunctionDefinition } from '../interfaces';
import moment from 'moment';
import { DataType, QueryError } from '../interfaces-private';
import { nullIsh } from '../utils';

export const dateFunctions: FunctionDefinition[] = [
    {
        name: 'to_date',
        args: [DataType.text, DataType.text],
        returns: DataType.date,
        implementation: (data, format) => {
            if (nullIsh(data) || nullIsh(format)) {
                return null; // if one argument is null => null
            }
            const ret = moment.utc(data, format);
            if (!ret.isValid()) {
                throw new QueryError(`The text '${data}' does not match the date format ${format}`);
            }
            return ret.toDate();
        },
    },
    {
        name: 'now',
        returns: DataType.timestamp,
        impure: true,
        implementation: () => new Date(),
    },
];
