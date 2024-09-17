import { stringFunctions } from './string';
import { dateFunctions } from './date';
import { systemFunctions } from './system';
import { sequenceFunctions } from './sequence-fns';
import { numberFunctions } from './numbers';
import { subqueryFunctions } from './subquery';


export const allFunctions = [
    ...stringFunctions
    , ... dateFunctions
    , ... systemFunctions
    , ... sequenceFunctions
    , ... numberFunctions
    , ... subqueryFunctions
]
