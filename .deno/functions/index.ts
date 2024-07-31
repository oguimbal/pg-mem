import { stringFunctions } from './string.ts';
import { dateFunctions } from './date.ts';
import { systemFunctions } from './system.ts';
import { sequenceFunctions } from './sequence-fns.ts';
import { numberFunctions } from './numbers.ts';


export const allFunctions = [
    ...stringFunctions
    , ... dateFunctions
    , ... systemFunctions
    , ... sequenceFunctions
    , ... numberFunctions
]