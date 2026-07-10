import { AggregationComputer, AggregationGroupComputer, IValue, nil, QueryError, _ISelection, _IType, _Transaction } from '../../interfaces-private.ts';
import { ExprCall } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { Types } from '../../datatypes/index.ts';
import { nullIsh } from '../../utils.ts';
import { withSelection } from '../../parser/context.ts';

/** generic comparison over pg-mem raw value representations */
function cmp(a: any, b: any): number {
    if (a instanceof Date) { a = a.getTime(); }
    if (b instanceof Date) { b = b.getTime(); }
    const na = typeof a === 'number' ? a : (typeof a === 'string' && a !== '' && !isNaN(Number(a)) ? Number(a) : a);
    const nb = typeof b === 'number' ? b : (typeof b === 'string' && b !== '' && !isNaN(Number(b)) ? Number(b) : b);
    if (typeof na === 'number' && typeof nb === 'number') { return na < nb ? -1 : na > nb ? 1 : 0; }
    return a < b ? -1 : a > b ? 1 : 0;
}

type Kind = 'percentile_cont' | 'percentile_disc' | 'mode';

class OrderedSetExpr implements AggregationComputer<any> {
    constructor(
        private kind: Kind,
        private orderExpr: IValue,
        private desc: boolean,
        private fraction: number | nil,
        private _type: _IType,
    ) { }

    get type(): _IType {
        // percentile_cont yields double precision; disc/mode yield the sort-expr type
        return this.kind === 'percentile_cont' ? Types.float : this._type;
    }

    createGroup(t: _Transaction): AggregationGroupComputer<any> {
        const values: any[] = [];
        return {
            feedItem: (item) => {
                const v = this.orderExpr.get(item, t);
                if (!nullIsh(v)) { values.push(v); }
            },
            finish: () => {
                if (!values.length) { return null; }
                values.sort((a, b) => this.desc ? cmp(b, a) : cmp(a, b));
                switch (this.kind) {
                    case 'percentile_cont': return this.contPercentile(values);
                    case 'percentile_disc': return this.discPercentile(values);
                    case 'mode': return this.mode(values);
                }
            },
        };
    }

    private contPercentile(sorted: any[]): number {
        const f = this.fraction!;
        const n = sorted.length;
        const rn = f * (n - 1);
        const lo = Math.floor(rn);
        const hi = Math.ceil(rn);
        const loV = Number(sorted[lo]);
        if (lo === hi) { return loV; }
        const hiV = Number(sorted[hi]);
        return loV + (rn - lo) * (hiV - loV);
    }

    private discPercentile(sorted: any[]): any {
        const f = this.fraction!;
        const n = sorted.length;
        // first value whose cumulative position reaches the fraction
        let idx = f <= 0 ? 0 : Math.ceil(f * n) - 1;
        if (idx < 0) { idx = 0; }
        if (idx >= n) { idx = n - 1; }
        return sorted[idx];
    }

    private mode(sorted: any[]): any {
        let best = sorted[0];
        let bestRun = 1;
        let cur = sorted[0];
        let run = 1;
        for (let i = 1; i < sorted.length; i++) {
            if (cmp(sorted[i], cur) === 0) {
                run++;
            } else {
                cur = sorted[i];
                run = 1;
            }
            if (run > bestRun) { bestRun = run; best = cur; }
        }
        return best;
    }
}

export function buildOrderedSetAgg(this: void, base: _ISelection, call: ExprCall, kind: Kind): AggregationComputer {
    return withSelection(base, () => {
        if (!call.withinGroup) {
            throw new QueryError(`${kind} requires a WITHIN GROUP (ORDER BY ...) clause`, '42809');
        }
        const orderExpr = buildValue(call.withinGroup.by);
        const desc = call.withinGroup.order === 'DESC';

        let fraction: number | nil = null;
        if (kind !== 'mode') {
            if (call.args.length !== 1) {
                throw new QueryError(`${kind} expects one argument, given ${call.args.length}`);
            }
            const fv = buildValue(call.args[0]);
            if (!fv.isConstant) {
                throw new QueryError(`${kind} fraction must be a constant`);
            }
            fraction = Number(fv.get());
            if (nullIsh(fraction) || fraction < 0 || fraction > 1) {
                throw new QueryError(`${kind} percentile value must be between 0 and 1`);
            }
        } else if (call.args.length) {
            throw new QueryError(`mode takes no arguments`);
        }

        return new OrderedSetExpr(kind, orderExpr, desc, fraction, orderExpr.type);
    });
}
