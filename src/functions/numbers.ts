import { DataType, FunctionDefinition } from "../interfaces";

export const numberFunctions: FunctionDefinition[] = [
    {
        name: 'greatest',
        args: [DataType.integer],
        argsVariadic: DataType.integer,
        returns: DataType.integer,
        implementation: (...args: number[]) => Math.max(...args),
    },
    {
        name: 'least',
        args: [DataType.integer],
        argsVariadic: DataType.integer,
        returns: DataType.integer,
        implementation: (...args: number[]) => Math.min(...args),
    },
]