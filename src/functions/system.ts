import { Types } from '../datatypes';
import { FunctionDefinition } from '../interfaces';

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
