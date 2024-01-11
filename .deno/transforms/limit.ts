import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats, nil } from '../interfaces-private.ts';
import { FilterBase } from './transform-base.ts';
import { LimitStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { withSelection } from '../parser/context.ts';

export function buildLimit(on: _ISelection, limit: LimitStatement) {
    return withSelection(on, () => {
        const l = limit.limit && buildValue(limit.limit);
        const o = limit.offset && buildValue(limit.offset);
        return new LimitFilter(on, l, o);
    });
}

class LimitFilter<T = any> extends FilterBase<T> {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        return this.selection.entropy(t);
    }

    hasItem(raw: T, t: _Transaction): boolean {
        return this.base.hasItem(raw, t);
    }

    constructor(private selection: _ISelection<T>, private take: IValue | nil, private skip: IValue | nil) {
        super(selection);
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<T> {
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