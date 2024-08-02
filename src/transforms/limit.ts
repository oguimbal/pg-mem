import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats, nil, Row } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { LimitStatement } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { withSelection } from '../parser/context';

export function buildLimit(on: _ISelection, limit: LimitStatement) {
    return withSelection(on, () => {
        const l = limit.limit && buildValue(limit.limit);
        const o = limit.offset && buildValue(limit.offset);
        return new LimitFilter(on, l, o);
    });
}

class LimitFilter extends FilterBase {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        return this.selection.entropy(t);
    }

    hasItem(raw: Row, t: _Transaction): boolean {
        return this.base.hasItem(raw, t);
    }

    constructor(private selection: _ISelection, private take: IValue | nil, private skip: IValue | nil) {
        super(selection);
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        let skip = this.skip?.get(null, t) ?? 0;
        let take = this.take?.get(null, t) ?? Number.MAX_SAFE_INTEGER;
        if (take <= 0) {
            return;
        }
        for (const raw of this.selection.enumerate(t)) {
            if (skip > 0) {
                skip--;
                continue;
            }
            yield raw;
            take--;
            if (!take) {
                return;
            }
        }
    }



    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'limit',
            take: this.take?.explain(e),
            skip: this.skip?.explain(e),
            on: this.selection.explain(e),
        };
    }
}