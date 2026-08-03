import { DataType, FunctionDefinition, nil } from "../interfaces";

export const numberFunctions: FunctionDefinition[] = [
    {
        name: 'greatest',
        args: [DataType.integer],
        argsVariadic: DataType.integer,
        returns: DataType.integer,
        allowNullArguments: true,
        implementation: (...args: (number | nil)[]) => {
            const values = args.filter((x): x is number => x !== null && x !== undefined);
            return values.length ? Math.max(...values) : null;
        },
    },
    {
        name: 'least',
        args: [DataType.integer],
        argsVariadic: DataType.integer,
        returns: DataType.integer,
        allowNullArguments: true,
        implementation: (...args: (number | nil)[]) => {
            const values = args.filter((x): x is number => x !== null && x !== undefined);
            return values.length ? Math.min(...values) : null;
        },
    },
]