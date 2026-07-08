import { DataType, FunctionDefinition, QueryError } from '../interfaces-private';
import { Types } from '../datatypes';
import { nullIsh } from '../utils';

function series(start: number, stop: number, step: number): number[] {
    if (!step) {
        throw new QueryError('step size cannot equal zero');
    }
    const ret: number[] = [];
    if (step > 0) {
        for (let i = start; i <= stop; i += step) {
            ret.push(i);
        }
    } else {
        for (let i = start; i >= stop; i += step) {
            ret.push(i);
        }
    }
    return ret;
}

export const arrayFunctions: FunctionDefinition[] = [
    {
        name: 'generate_series',
        args: [DataType.integer, DataType.integer],
        returns: Types.integer.asArray(),
        setReturning: true,
        implementation: (start: number, stop: number) => series(start, stop, 1),
    },
    {
        name: 'generate_series',
        args: [DataType.integer, DataType.integer, DataType.integer],
        returns: Types.integer.asArray(),
        setReturning: true,
        implementation: series,
    },
    {
        name: 'string_to_array',
        args: [DataType.text, DataType.text],
        returns: Types.text().asArray(),
        allowNullArguments: true,
        implementation: (str: string | null, sep: string | null) => {
            if (nullIsh(str)) {
                return null;
            }
            if (nullIsh(sep)) {
                // pg: null delimiter splits into individual characters
                return [...str!];
            }
            // pg: empty delimiter yields the whole string as a single field
            return sep === '' ? [str] : str!.split(sep!);
        },
    },
];
