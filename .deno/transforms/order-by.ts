import { IValue, _ISelection, _Transaction, _Explainer, _SelectExplanation, Stats, _IAggregation } from '../interfaces-private.ts';
import { FilterBase } from './transform-base.ts';
import { OrderByStatement, ExprCall } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { nullIsh } from '../utils.ts';
import { withSelection } from '../parser/context.ts';

export function buildOrderBy(on: _ISelection, order: OrderByStatement[]) {
    return new OrderBy(on, order);
}

class OrderBy<T> extends FilterBase<any> implements _IAggregation {
    order: {
        by: IValue<any>;
        order: 'ASC' | 'DESC';
        nullsLast: boolean;
    }[];

    get index() {
        return null;
    }

    isAggregation() {
        return this.selection.isAggregation();
    }

    getAggregation(name: string, call: ExprCall): IValue {
        return this.asAggreg.getAggregation(name, call);
    }

    checkIfIsKey(got: IValue<any>): IValue<any> {
        return this.asAggreg.checkIfIsKey(got);
    }

    private get asAggreg(): _IAggregation {

        if (!this.selection.isAggregation()) {
            throw new Error('Not an aggregation');
        }
        return this.selection;
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
        this.order = withSelection(selection,
            () => order.map(x => {
                const order = x.order ?? 'ASC';
                return ({
                    by: buildValue(x.by),
                    order,
                    nullsLast: order === 'ASC' ? x.nulls !== 'FIRST' : x.nulls === 'LAST',
                });
            }));
    }


    stats(t: _Transaction): Stats | null {
        return this.base.stats(t);
    }


    getIndex(...forValue: IValue<any>[]) {
        // same index as underlying selection, given that ordering does not modify indices.
        return this.base.getIndex(...forValue);
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
                    return nb === o.nullsLast ? -1 : 1;
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