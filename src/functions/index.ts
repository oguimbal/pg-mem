import { stringFunctions } from './string';
import { dateFunctions } from './date';
import { systemFunctions } from './system';
import { sequenceFunctions } from './sequence-fns';


export const allFunctions = [
    ...stringFunctions
    , ... dateFunctions
    , ... systemFunctions
    , ... sequenceFunctions
]