import { FunctionDefinition } from '../interfaces.ts';
import { DataType } from '../interfaces-private.ts';

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
