import { Expr } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { IValue, Stats, _Explainer, _ISelection, _SelectExplanation, _Transaction } from '../interfaces-private.ts';
import { FilterBase } from './transform-base.ts';
import objectHash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';
import { withSelection } from '../parser/context.ts';

export function buildDistinct(on: _ISelection, exprs?: Expr[]) {
    return withSelection(on, () => {
        const vals = exprs && exprs.length > 0
            ? exprs.map(v => buildValue(v))
            : on.columns
        return new Distinct(on, vals);
    });
}


// todo: use indices to optimize this (avoid iterating everything)

class Distinct<T> extends FilterBase<any> {

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        // cant foresight how many items will be filtered
        //  => just asumme nothing will be.
        return this.base.entropy(t);
    }

    hasItem(raw: T, t: _Transaction): boolean {
        return this.base.hasItem(raw, t);
    }

    constructor(selection: _ISelection, private exprs: ReadonlyArray<IValue>) {
        super(selection);
    }

    stats(t: _Transaction): Stats | null {
        return this.base.stats(t);
    }

    *enumerate(t: _Transaction): Iterable<T> {
        const got = new Set();
        for (const i of this.base.enumerate(t)) {
            const vals = this.exprs.map(v => v.type.hash(v.get(i, t)));
            const hash = vals.length === 1 ? vals[0] : objectHash(vals);
            if (got.has(hash)) {
                continue;
            }
            got.add(hash);
            yield i;
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'distinct',
            of: this.base.explain(e),
        };
    }
}