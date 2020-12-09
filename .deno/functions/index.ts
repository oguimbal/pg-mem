import { stringFunctions } from './string.ts';
import { dateFunctions } from './date.ts';
import { systemFunctions } from './system.ts';
import { sequenceFunctions } from './sequence-fns.ts';


export const allFunctions = [
    ...stringFunctions
    , ... dateFunctions
    , ... systemFunctions
    , ... sequenceFunctions
]