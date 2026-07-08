import { TransformBase } from './transform-base';
import { _ISelection, _Transaction, IValue, _IIndex, _Explainer, _SelectExplanation, _IType, Stats, Row } from '../interfaces-private';
import { CallOverFrame, Expr, ExprCall, ExprRef, FrameBound, nil, OrderByStatement } from 'pgsql-ast-parser';
import { buildValue } from '../parser/expression-builder';
import { NotSupported, QueryError } from '../interfaces';
import { Types } from '../datatypes';
import hash from 'object-hash';
import { Evaluator } from '../evaluator';
import { Selection } from './selection';
import { buildCtx, withSelection } from '../parser/context';
import { asSingleQName, nullIsh } from '../utils';

// Window functions compute over the source rows *after* where/group-by, but yield one
// value per row (unlike aggregations, which collapse groups). This transform annotates
// each source row with the computed value of every registered window call, under a
// per-call symbol that the call's Evaluator reads back.
//
// Frames: only the pg default is implemented — RANGE BETWEEN UNBOUNDED PRECEDING AND
// CURRENT ROW when the window has an ORDER BY (i.e. running aggregates over peer
// groups), the whole partition otherwise. Explicit frame clauses fail at parse time.

export function buildWindow(on: _ISelection): WindowedSelection {
    return new WindowedSelection(on);
}

/** Does any of those select-column expressions contain a window call (fn() OVER ...) ? */
export function exprsHaveWindow(exprs: (Expr | nil)[] | nil): boolean {
    const visit = (n: any): boolean => {
        if (!n || typeof n !== 'object') {
            return false;
        }
        if (Array.isArray(n)) {
            return n.some(visit);
        }
        if ((n as ExprCall).type === 'call' && (n as ExprCall).over) {
            return true;
        }
        return Object.values(n).some(visit);
    };
    return !!exprs?.some(x => visit(x));
}

/** Finds the WindowedSelection the currently-built expression can register calls on */
export function getWindower(): WindowedSelection | null {
    const on = buildCtx().selection;
    if (on instanceof WindowedSelection) {
        return on;
    }
    if (on instanceof Selection && on.base instanceof WindowedSelection) {
        return on.base;
    }
    return null;
}

interface WindowInstance {
    id: symbol;
    fname: string;
    getter: IValue;
    /** call arguments, evaluated on base rows ('*' args are stripped) */
    args: IValue[];
    countStar: boolean;
    partitionBy: IValue[];
    orderBy: { by: IValue; order: 'ASC' | 'DESC'; nullsLast: boolean }[];
    /** explicit frame clause, if any (null = pg default frame) */
    frame: WindowFrame | null;
}

interface WindowFrame {
    unit: 'rows' | 'range' | 'groups';
    start: FrameBoundSpec;
    end: FrameBoundSpec;
}

interface FrameBoundSpec {
    type: 'unbounded preceding' | 'unbounded following' | 'current row' | 'preceding' | 'following';
    value?: IValue;
}

function buildFrame(frame: CallOverFrame): WindowFrame {
    const buildBound = (b: FrameBound): FrameBoundSpec => ({
        type: b.type,
        value: b.value ? buildValue(b.value).cast(Types.integer) : undefined,
    });
    const ret: WindowFrame = {
        unit: frame.unit,
        start: buildBound(frame.start),
        // pg: a single bound means "BETWEEN <bound> AND CURRENT ROW"
        end: buildBound(frame.end ?? { type: 'current row' }),
    };
    if (ret.start.type === 'unbounded following') {
        throw new QueryError('frame start cannot be UNBOUNDED FOLLOWING');
    }
    if (ret.end.type === 'unbounded preceding') {
        throw new QueryError('frame end cannot be UNBOUNDED PRECEDING');
    }
    if (ret.unit === 'range' && (ret.start.value || ret.end.value)) {
        throw new NotSupported('RANGE frames with offset PRECEDING/FOLLOWING');
    }
    return ret;
}

let winCnt = 0;

export class WindowedSelection extends TransformBase implements _ISelection {

    private windows = new Map<string, WindowInstance>();
    private readonly winId = winCnt++;

    get columns(): readonly IValue[] {
        return this.base.columns;
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        return this.base.getColumn(column, nullIfNotFound);
    }

    entropy(t: _Transaction): number {
        return this.base.entropy(t);
    }

    stats(t: _Transaction): Stats | null {
        return this.base.stats(t);
    }

    hasItem(value: Row, t: _Transaction): boolean {
        return this.base.hasItem(value, t);
    }

    getIndex(...forValue: IValue[]): _IIndex | nil {
        // an index would bypass this transform's row annotation
        return null;
    }

    getWindowValue(call: ExprCall): IValue {
        const hashed = hash(call);
        const existing = this.windows.get(hashed);
        if (existing) {
            return existing.getter;
        }

        const fname = asSingleQName(call.function, 'pg_catalog');
        if (!fname) {
            throw new NotSupported(`window function ${JSON.stringify(call.function)}`);
        }
        const argExprs = call.args.filter(a => !(a.type === 'ref' && a.name === '*'));
        const over = call.over!;
        const built = withSelection(this.base, () => ({
            args: argExprs.map(a => buildValue(a)),
            partitionBy: (over.partitionBy ?? []).map(p => buildValue(p)),
            orderBy: (over.orderBy ?? []).map((o: OrderByStatement) => {
                const order = o.order ?? 'ASC';
                return {
                    by: buildValue(o.by),
                    order,
                    nullsLast: order === 'ASC' ? o.nulls !== 'FIRST' : o.nulls === 'LAST',
                };
            }),
            frame: over.frame ? buildFrame(over.frame) : null,
        }));

        const id = Symbol(fname);
        const getter = new Evaluator(
            windowReturnType(fname, built.args)
            , null
            , hashed
            , []
            , raw => raw[id]
            , { forceNotConstant: true });

        this.windows.set(hashed, {
            id,
            fname,
            getter,
            countStar: argExprs.length !== call.args.length,
            ...built,
        });
        return getter;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        if (!this.windows.size) {
            yield* this.base.enumerate(t);
            return;
        }
        // shallow copies so annotations never leak onto rows owned by the data store
        const rows = [...this.base.enumerate(t)].map(r => ({ ...(r as any) }));
        for (const w of this.windows.values()) {
            const partitions = new Map<string, any[]>();
            for (const r of rows) {
                const key = hash(w.partitionBy.map(p => p.get(r, t)));
                let part = partitions.get(key);
                if (!part) {
                    partitions.set(key, part = []);
                }
                part.push(r);
            }
            for (const partition of partitions.values()) {
                this.sortPartition(partition, w, t);
                this.computeWindow(partition, w, t);
            }
        }
        // rows are yielded in source order: computing windows must not reorder the output
        yield* rows;
    }

    private sortPartition(partition: any[], w: WindowInstance, t: _Transaction): void {
        if (!w.orderBy.length) {
            return;
        }
        partition.sort((a, b) => {
            for (const o of w.orderBy) {
                const av = o.by.get(a, t);
                const bv = o.by.get(b, t);
                const na = nullIsh(av);
                const nb = nullIsh(bv);
                if (na && nb) {
                    continue;
                }
                if (na || nb) {
                    return nb === o.nullsLast ? -1 : 1;
                }
                if (o.by.type.equals(av, bv)) {
                    continue;
                }
                if (o.by.type.gt(av, bv)) {
                    return o.order === 'ASC' ? 1 : -1;
                }
                return o.order === 'ASC' ? -1 : 1;
            }
            return 0;
        });
    }

    /** Splits an ordered partition into groups of peer rows (equal ORDER BY keys).
     * Without an ORDER BY, every row is a peer of every other. */
    private *peerGroups(partition: any[], w: WindowInstance, t: _Transaction): Iterable<any[]> {
        if (!w.orderBy.length) {
            yield partition;
            return;
        }
        let group: any[] = [partition[0]];
        for (let i = 1; i < partition.length; i++) {
            if (this.arePeers(partition[i - 1], partition[i], w, t)) {
                group.push(partition[i]);
            } else {
                yield group;
                group = [partition[i]];
            }
        }
        yield group;
    }

    /** Per-row inclusive [from, to] row-index bounds of an explicit frame.
     * from > to denotes an empty frame. */
    private frameBounds(partition: any[], w: WindowInstance, t: _Transaction): [number, number][] {
        const frame = w.frame!;
        const len = partition.length;

        // peer-group structure, needed by range & groups units
        let groupOf: number[] = [];
        const groups: [number, number][] = [];
        if (frame.unit !== 'rows') {
            let start = 0;
            for (let i = 0; i < len; i++) {
                if (i > 0 && !this.arePeers(partition[i - 1], partition[i], w, t)) {
                    groups.push([start, i - 1]);
                    start = i;
                }
                groupOf.push(groups.length);
            }
            groups.push([start, len - 1]);
        }

        const offset = (b: FrameBoundSpec): number => {
            const v = b.value!.get(partition[0], t);
            if (nullIsh(v) || v < 0) {
                throw new QueryError(`frame ${b.type} offset must not be null or negative`);
            }
            return v;
        };

        const rowBound = (b: FrameBoundSpec, i: number): number => {
            switch (b.type) {
                case 'unbounded preceding': return 0;
                case 'unbounded following': return len - 1;
                case 'current row': return i;
                case 'preceding': return i - offset(b);
                case 'following': return i + offset(b);
            }
        };

        // groups & range bounds resolve to a peer group, then take its start or end row
        const groupBound = (b: FrameBoundSpec, gIdx: number, isStart: boolean): number => {
            switch (b.type) {
                case 'unbounded preceding': return 0;
                case 'unbounded following': return len - 1;
                case 'current row': return groups[gIdx][isStart ? 0 : 1];
                case 'preceding':
                case 'following': {
                    const g2 = gIdx + (b.type === 'following' ? 1 : -1) * offset(b);
                    if (g2 < 0) {
                        return isStart ? 0 : -1; // before the partition
                    }
                    if (g2 >= groups.length) {
                        return isStart ? len : len - 1; // past the partition
                    }
                    return groups[g2][isStart ? 0 : 1];
                }
            }
        };

        return partition.map((_, i) => {
            let from: number;
            let to: number;
            if (frame.unit === 'rows') {
                from = rowBound(frame.start, i);
                to = rowBound(frame.end, i);
            } else {
                from = groupBound(frame.start, groupOf[i], true);
                to = groupBound(frame.end, groupOf[i], false);
            }
            return [Math.max(0, from), Math.min(len - 1, to)] as [number, number];
        });
    }

    private arePeers(a: any, b: any, w: WindowInstance, t: _Transaction): boolean {
        return w.orderBy.every(o => {
            const av = o.by.get(a, t);
            const bv = o.by.get(b, t);
            const na = nullIsh(av);
            const nb = nullIsh(bv);
            return na || nb ? na === nb : o.by.type.equals(av, bv);
        });
    }

    private computeWindow(partition: any[], w: WindowInstance, t: _Transaction): void {
        const { id, fname, args } = w;
        switch (fname) {
            case 'row_number': {
                partition.forEach((r, i) => r[id] = i + 1);
                return;
            }
            case 'rank': {
                let start = 1;
                for (const g of this.peerGroups(partition, w, t)) {
                    for (const r of g) {
                        r[id] = start;
                    }
                    start += g.length;
                }
                return;
            }
            case 'dense_rank': {
                let rank = 1;
                for (const g of this.peerGroups(partition, w, t)) {
                    for (const r of g) {
                        r[id] = rank;
                    }
                    rank++;
                }
                return;
            }
            case 'ntile': {
                if (args.length !== 1) {
                    throw new QueryError('ntile expects 1 argument');
                }
                const buckets = args[0].get(partition[0], t);
                if (nullIsh(buckets)) {
                    partition.forEach(r => r[id] = null);
                    return;
                }
                if (buckets <= 0) {
                    throw new QueryError('argument of ntile must be greater than zero');
                }
                const small = Math.floor(partition.length / buckets);
                const extra = partition.length % buckets;
                let i = 0;
                for (let b = 1; b <= buckets && i < partition.length; b++) {
                    const size = small + (b <= extra ? 1 : 0);
                    for (let k = 0; k < size; k++) {
                        partition[i++][id] = b;
                    }
                }
                return;
            }
            case 'lag':
            case 'lead': {
                if (!args.length || args.length > 3) {
                    throw new QueryError(`${fname} expects 1 to 3 arguments`);
                }
                partition.forEach((r, i) => {
                    const offRaw = args.length >= 2 ? args[1].get(r, t) : 1;
                    if (nullIsh(offRaw)) {
                        r[id] = null;
                        return;
                    }
                    const j = fname === 'lag' ? i - offRaw : i + offRaw;
                    r[id] = j >= 0 && j < partition.length
                        ? args[0].get(partition[j], t)
                        : args.length >= 3 ? args[2].get(r, t) : null;
                });
                return;
            }
            case 'first_value': {
                if (w.frame) {
                    const frames = this.frameBounds(partition, w, t);
                    partition.forEach((r, i) => {
                        const [from, to] = frames[i];
                        r[id] = from > to ? null : args[0].get(partition[from], t);
                    });
                    return;
                }
                const v = args[0].get(partition[0], t);
                partition.forEach(r => r[id] = v);
                return;
            }
            case 'last_value': {
                if (w.frame) {
                    const frames = this.frameBounds(partition, w, t);
                    partition.forEach((r, i) => {
                        const [from, to] = frames[i];
                        r[id] = from > to ? null : args[0].get(partition[to], t);
                    });
                    return;
                }
                // default frame ends at the current row's last peer
                for (const g of this.peerGroups(partition, w, t)) {
                    const v = args[0].get(g[g.length - 1], t);
                    for (const r of g) {
                        r[id] = v;
                    }
                }
                return;
            }
            case 'count':
            case 'sum':
            case 'avg':
            case 'min':
            case 'max': {
                const feedOf = (r: any) => w.countStar || !args.length ? 1 : args[0].get(r, t);
                if (w.frame) {
                    // explicit frame: computed row by row
                    const frames = this.frameBounds(partition, w, t);
                    partition.forEach((r, i) => {
                        const acc = windowAggregator(fname, w);
                        const [from, to] = frames[i];
                        for (let j = from; j <= to; j++) {
                            acc.feed(feedOf(partition[j]));
                        }
                        r[id] = acc.value();
                    });
                    return;
                }
                // default frame: running aggregate, accumulated peer group by peer group
                const acc = windowAggregator(fname, w);
                for (const g of this.peerGroups(partition, w, t)) {
                    for (const r of g) {
                        acc.feed(feedOf(r));
                    }
                    const v = acc.value();
                    for (const r of g) {
                        r[id] = v;
                    }
                }
                return;
            }
            default:
                throw new NotSupported(`window function ${fname}()`);
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'map',
            id: e.idFor(this),
            select: [],
            of: this.base.explain(e),
        };
    }
}

function windowReturnType(fname: string, args: IValue[]): _IType {
    switch (fname) {
        case 'row_number':
        case 'rank':
        case 'dense_rank':
        case 'count':
            return Types.bigint;
        case 'ntile':
            return Types.integer;
        case 'avg':
            return Types.float;
        case 'sum':
            return args[0]?.type ?? Types.float;
        default:
            return args[0]?.type ?? Types.null;
    }
}

function windowAggregator(fname: string, w: WindowInstance) {
    const star = w.countStar || !w.args.length;
    const argType = w.args[0]?.type;
    let count = 0;
    let sum = 0;
    let best: any = null;
    return {
        feed(v: any) {
            if (!star && nullIsh(v)) {
                return;
            }
            count++;
            if (typeof v === 'number') {
                sum += v;
            }
            if (fname === 'min' || fname === 'max') {
                if (best === null || (fname === 'max' ? argType!.gt(v, best) : argType!.lt(v, best))) {
                    best = v;
                }
            }
        },
        value(): any {
            switch (fname) {
                case 'count':
                    return count;
                case 'sum':
                    return count ? sum : null;
                case 'avg':
                    return count ? sum / count : null;
                default:
                    return best;
            }
        },
    };
}
