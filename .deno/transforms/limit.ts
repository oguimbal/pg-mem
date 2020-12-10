import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats } from '../interfaces-private.ts';
import { FilterBase } from './transform-base.ts';
import { LimitStatement } from 'https://deno.land/x/pgsql_ast_parser@1.3.7/mod.ts';

export function buildLimit(on: _ISelection, limit: LimitStatement) {
    return new LimitFilter(on, limit.limit!, limit.offset!);
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

    constructor(private selection: _ISelection<T>, private take: number, private skip: number) {
        super(selection);
    }


    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<T> {
        let skip = this.skip;
        let take = this.take;
        if (!take) {
            return;
        }
        for (const raw of this.selection.enumerate(t)) {
            if (skip) {
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
            take: this.take,
            skip: this.skip,
            on: this.selection.explain(e),
        };
    }
}