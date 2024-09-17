import { FunctionDefinition } from '../interfaces';
import moment from 'moment-timezone';
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
        }
    },
    {
        name: 'now',
        returns: DataType.timestamptz,
        impure: true,
        implementation: () => new Date(),
    },
    {
        name: 'timezone',
        args: [DataType.text, DataType.timestamptz],
        returns: DataType.timestamptz,
        implementation: (timezone: string, timestamptz: Date): Date =>
            moment(timestamptz).tz(timezone).toDate(),
    },
];
