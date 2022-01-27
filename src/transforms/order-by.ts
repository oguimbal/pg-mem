import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { OrderByStatement } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { nullIsh } from '../utils';

export function buildOrderBy(on: _ISelection, order: OrderByStatement[]) {
    return new OrderBy(on, order);
}

class OrderBy<T> extends FilterBase<any> {
    order: {
        by: IValue<any>;
        order: 'ASC' | 'DESC';
        nullsLast: boolean;
    }[];

    get index() {
        return null;
    }

    entropy(t: _Transaction) {
        const ret = this.selection.entropy(t);
        // sort algorithm is n*log(n)
        return ret * Math.log(ret + 1);
    }

    hasItem(raw: T, t: _Transaction): boolean {
        return this.base.hasItem(raw, t);
    }

    constructor(private selection: _ISelection<T>, order: OrderByStatement[]) {
        super(selection);
        this.order = order.map(x => ({
            by: buildValue(selection, x.by),
            order: x.order ?? 'ASC',
            nullsLast: x.nulls === 'LAST',
        }))
    }


    stats(t: _Transaction): Stats | null {
        return this.base.stats(t);
    }

    enumerate(t: _Transaction): Iterable<T> {
        const all = [...this.base.enumerate(t)];
        all.sort((a, b) => {
            for (const o of this.order) {
                const aval = o.by.get(a, t);
                const bval = o.by.get(b, t);
                const na = nullIsh(aval);
                const nb = nullIsh(bval);
                if (na && nb) {
                    continue;
                }
                if (na || nb) {
                    return (o.order === 'ASC') === (nb === o.nullsLast) ? 1 : -1;
                }
                if (o.by.type.equals(aval, bval)) {
                    continue;
                }
                if (o.by.type.gt(aval, bval)) {
                    return o.order === 'ASC' ? 1 : -1;
                }
                return o.order === 'ASC' ? -1 : 1;
            }
            return 0;
        });
        return all;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'orderBy',
            of: this.selection.explain(e),
        };
    }
}