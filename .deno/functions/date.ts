import { FunctionDefinition } from '../interfaces.ts';
import moment from 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts';
import { DataType, QueryError } from '../interfaces-private.ts';
import { nullIsh } from '../utils.ts';


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
        returns: DataType.timestamp,
        impure: true,
        implementation: () => new Date(),
    },
];