import { stringFunctions } from './string.ts';
import { dateFunctions } from './date.ts';
import { systemFunctions } from './system.ts';
import { sequenceFunctions } from './sequence-fns.ts';
import { numberFunctions } from './numbers.ts';
import { subqueryFunctions } from './subquery.ts';


export const allFunctions = [
    ...stringFunctions
    , ... dateFunctions
    , ... systemFunctions
    , ... sequenceFunctions
    , ... numberFunctions
    , ... subqueryFunctions
]
