import { stringFunctions } from './string';
import { dateFunctions } from './date';
import { systemFunctions } from './system';
import { sequenceFunctions } from './sequence-fns';
import { numberFunctions } from './numbers';
import { subqueryFunctions } from './subquery';
import { arrayFunctions } from './array';
import { jsonFunctions } from './json';


export const allFunctions = [
    ...stringFunctions
    , ... dateFunctions
    , ... systemFunctions
    , ... sequenceFunctions
    , ... numberFunctions
    , ... subqueryFunctions
    , ... arrayFunctions
    , ... jsonFunctions
]
