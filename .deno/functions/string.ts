import { DataType, FunctionDefinition } from '../interfaces-private.ts';

export const stringFunctions: FunctionDefinition[] = [
    {
        name: 'lower',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => x?.toLowerCase(),
    },
    {
        name: 'upper',
        args: [DataType.text],
        returns: DataType.text,
        implementation: (x: string) => x?.toUpperCase(),
    },
    {
        name: 'concat',
        args: [DataType.text],
        argsVariadic: DataType.text,
        returns: DataType.text,
        allowNullArguments: true,
        implementation: (...x: string[]) => x?.join(''),
    },
    {
        name: 'concat_ws',
        args: [DataType.text],
        argsVariadic: DataType.text,
        returns: DataType.text,
        allowNullArguments: true,
        implementation: (separator: string, ...x: string[]) => x?.join(separator),
    },
]
