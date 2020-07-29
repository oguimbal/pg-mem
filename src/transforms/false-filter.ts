import { _ISelection, IValue, _Explainer, _SelectExplanation, _Transaction, Stats } from '../interfaces-private';
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

    stats(t: _Transaction): Stats | null {
        return {
            count: 0,
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'empty',
        };
    }
}