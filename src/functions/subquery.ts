import { FunctionDefinition } from '../interfaces';
import { DataType } from '../interfaces-private';

export const subqueryFunctions: FunctionDefinition[] = [
    {
        name: 'exists',
        args: [DataType.integer],
        argsVariadic: DataType.integer,
        returns: DataType.bool,
        allowNullArguments: true,
        impure: true,
        implementation: (...items: number[]) => items?.some?.(Boolean) ?? false,
    },
];
