import { _ISelection, IValue, _Explainer, _SelectExplanation, _Transaction, Stats, Row } from '../interfaces-private.ts';
import { FilterBase } from './transform-base.ts';

export class FalseFilter extends FilterBase {

    get index() {
        return null;
    }

    entropy() {
        return 0;
    }

    hasItem() {
        return false;
    }

    enumerate(): Iterable<Row> {
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