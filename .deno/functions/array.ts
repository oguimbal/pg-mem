import { DataType, FunctionDefinition, QueryError } from '../interfaces-private.ts';
import { Types } from '../datatypes/index.ts';
import { nullIsh, dateAddInterval } from '../utils.ts';
import { Interval } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';

function dateSeries(start: Date, stop: Date, step: Interval): Date[] {
    const stepped = dateAddInterval(start, step, 1);
    if (stepped.getTime() === start.getTime()) {
        throw new QueryError('step size cannot equal zero');
    }
    const forward = stepped.getTime() > start.getTime();
    const stopMs = stop.getTime();
    const ret: Date[] = [];
    let cur = start;
    while (forward ? cur.getTime() <= stopMs : cur.getTime() >= stopMs) {
        ret.push(cur);
        cur = dateAddInterval(cur, step, 1);
    }
    return ret;
}

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
        name: 'generate_series',
        args: [Types.timestamp(), Types.timestamp(), Types.interval],
        returns: Types.timestamp().asArray(),
        setReturning: true,
        implementation: dateSeries,
    },
    {
        name: 'generate_series',
        args: [Types.timestamptz(), Types.timestamptz(), Types.interval],
        returns: Types.timestamptz().asArray(),
        setReturning: true,
        implementation: dateSeries,
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
