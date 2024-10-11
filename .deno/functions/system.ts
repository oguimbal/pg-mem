import { Types } from '../datatypes/index.ts';
import { FunctionDefinition } from '../interfaces.ts';

export const systemFunctions: FunctionDefinition[] = [
    {
        // ugly hack...
        name: 'current_schema',
        returns: Types.text(),
        implementation: () => 'public',
    },
    {
        name: 'obj_description',
        args: [Types.regclass, Types.text()],
        returns: Types.null,
        implementation: () => null
    },
]
