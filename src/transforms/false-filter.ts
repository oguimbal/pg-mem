import { _ISelection, IValue, _Explainer, _SelectExplanation } from '../interfaces-private';
import { FilterBase } from './transform-base';

export class FalseFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    entropy() {
        return 0;
    }

    hasItem() {
        return false;
    }

    enumerate(): Iterable<T> {
        return [];
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            type: 'empty',
        };
    }
}