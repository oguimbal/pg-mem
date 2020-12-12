import { _ISelection, IValue, _IIndex, _IDb, setId, getId, _Transaction, _ISchema, _SelectExplanation, _Explainer, IndexExpression, IndexOp, IndexKey, _IndexExplanation, Stats } from '../interfaces-private';
import { buildValue, uncache } from '../predicate';
import { QueryError, ColumnNotFound, NotSupported, nil } from '../interfaces';
import { DataSourceBase } from './transform-base';
import { Expr } from 'pgsql-ast-parser';
import { nullIsh } from '../utils';
import { Types } from '../datatypes';

let jCnt = 0;

interface JoinRaw<TLeft, TRight> {
    '>restrictive': TLeft;
    '>joined': TRight;
}
interface JoinStrategy {
    iterate: _ISelection<any>;
    iterateSide: 'joined' | 'restrictive';
    joinIndex: _IIndex<any>;
    onValue: IValue;
    othersPredicate?: IValue<any>;
}


function* extractAnds(this: void, on: Expr): Iterable<Expr> {
    if (on.type === 'binary' && on.op === 'AND') {
        yield* extractAnds(on.left);
        yield* extractAnds(on.right);
        return;
    }
    yield on;
}

function chooseStrategy(this: void, t: _Transaction, strategies: JoinStrategy[]) {
    strategies.sort((a, b) => a.iterate.entropy(t) > b.iterate.entropy(t) ? 1 : -1);
    return strategies[0];
}

export class JoinSelection<TLeft = any, TRight = any> extends DataSourceBase<JoinRaw<TLeft, TRight>> {

    private _columns: IValue<any>[] = [];
    private seqScanExpression: IValue<any>;
    private joinId: number;
    private columnsMappingParentToThis = new Map<IValue, IValue>();
    private columnsMappingThisToParent = new Map<IValue, {
        side: 'joined' | 'restrictive';
        col: IValue;
    }>();
    private indexCache = new Map<IValue, _IIndex>();
    strategies: JoinStrategy[] = [];
    private building = false;


    isOriginOf(a: IValue<any>): boolean {
        return this.joined.isOriginOf(a) || this.restrictive.isOriginOf(a);
    }

    get columns(): IValue<any>[] {
        return this._columns;
    }

    private get restrictiveColumns() {
        return this.restrictive.columns
    }

    private get joinedColumns() {
        return this.joined.columns
    }

    entropy(t: _Transaction): number {
        const strategy = chooseStrategy(t, this.strategies);
        if (!strategy) {
            // catastophic join... very high entropy...
            return this.restrictive.entropy(t) * this.joined.entropy(t);
        }

        // todo: multiply that by the mean count per keys in strategy.joinIndex ?
        const ret = strategy.iterate.entropy(t);
        return ret;
    }

    constructor(db: _ISchema
        , readonly restrictive: _ISelection<TLeft>
        , readonly joined: _ISelection<TRight>
        , on: Expr
        , private innerJoin: boolean) {
        super(db);

        if (!on) {
            throw new Error('Unspecified join ON clause');
        }

        this.joinId = jCnt++;
        for (const c of this.restrictiveColumns) {
            const nc = c.setWrapper(this, x => (x as any)['>restrictive']);
            this._columns.push(nc);
            this.columnsMappingParentToThis.set(c, nc);
            this.columnsMappingThisToParent.set(nc, {
                col: c,
                side: 'restrictive'
            });
            this.columnsMappingParentToThis
        }
        for (const c of this.joinedColumns) {
            const nc = c.setWrapper(this, x => (x as any)['>joined']);
            this._columns.push(nc);
            this.columnsMappingParentToThis.set(c, nc);
            this.columnsMappingThisToParent.set(nc, {
                col: c,
                side: 'joined',
            });
        }

        this.fetchStrategies(on);

        uncache(this);
        this.seqScanExpression = buildValue(this, on).convert(Types.bool);
    }

    private fetchStrategies(on: Expr) {
        const all = [...extractAnds(on)];
        for (let i = 0; i < all.length; i++) {
            const thisOne = all[i];
            const strats = [...this.fetchEqStrategy(thisOne)];
            if (!strats.length) {
                continue;
            }
            const others = [...all.slice(0, i), ...all.slice(i + 1)];
            if (others.length) {
                const and = others.slice(1)
                    .reduce<Expr>((v, c) => ({
                        type: 'binary',
                        left: c,
                        right: v,
                        op: 'AND',
                    }), others[0]);
                const othersPredicate = buildValue(this, and);
                for (const s of strats) {
                    s.othersPredicate = othersPredicate;
                }
            }
            this.strategies.push(...strats);
        }
    }

    private *fetchEqStrategy(on: Expr): Iterable<JoinStrategy> {
        if (on.type !== 'binary' || on.op !== '=') {
            return;
        }
        this.building = true;
        const a = buildValue(this, on.left);
        const b = buildValue(this, on.right);
        this.building = false;
        let restrictedVal: IValue | undefined = undefined;
        let joinedVal: IValue | undefined = undefined;

        // const aIndex = a.wrappedOrigin?.getIndex()
        if (this.restrictive.isOriginOf(a) && this.joined.isOriginOf(b)) {
            restrictedVal = a;
            joinedVal = b;
        } else if (this.restrictive.isOriginOf(b) && this.joined.isOriginOf(a)) {
            restrictedVal = b;
            joinedVal = a;
        }

        let processInner = this.innerJoin;
        let iterateSide: 'restrictive' | 'joined' = 'restrictive'
        while (restrictedVal && joinedVal) {
            // can always iterat on restricted value & use joined table foreign index
            const jindex = joinedVal.index;
            if (jindex && jindex.expressions.length === 1) {
                yield {
                    iterate: iterateSide === 'restrictive' ? this.restrictive : this.joined,
                    iterateSide,
                    onValue: restrictedVal,
                    joinIndex: jindex,
                }
            }
            if (!processInner) {
                break;
            }
            // if is an inner join, then both sides can be interverted
            processInner = false;
            const t = restrictedVal;
            restrictedVal = joinedVal;
            joinedVal = t;
            iterateSide = 'joined';
        }
    }

    getColumn(column: string): IValue;
    getColumn(column: string, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> | nil {
        let onLeft = this.restrictive.getColumn(column, true);
        let onRight = this.joined.getColumn(column, true);
        if (!onLeft && !onRight) {
            if (nullIfNotFound) {
                return null;
            }
            throw new ColumnNotFound(column);
        }
        if (!!onLeft && !!onRight) {
            throw new QueryError(`column reference "${column}" is ambiguous`);
        }
        if (this.building) {
            return onLeft ?? onRight;
        }
        const mapped = this.columnsMappingParentToThis.get(onLeft ?? onRight!);
        if (!mapped) {
            throw new Error('Corrupted join');
        }
        return mapped;
    }

    stats(t: _Transaction): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<any> {
        const strategy = chooseStrategy(t, this.strategies);
        if (strategy) {
            // choose the iterator that has less values
            // find the right value using index
            for (const l of strategy.iterate.enumerate(t)) {
                yield* this.iterateStrategyItem(l, strategy, t);
            }
        } else {
            // perform a seq scan
            this.db.raiseGlobal('catastrophic-join-optimization');
            const others = [...this.joined.enumerate(t)];
            for (const l of this.restrictive.enumerate(t)) {
                yield* this.iterateCatastrophicItem(l, others, 'restrictive', t);
            }
        }
    }

    *iterateCatastrophicItem(item: any, others: any[], side: 'joined' | 'restrictive', t: _Transaction) {
        const { template, buildItem } = this.builder(item, side);
        let yielded = false;
        for (const cr of others) {
            const combined = buildItem(cr);
            const result = this.seqScanExpression.get(combined, t);
            if (result) {
                yielded = true;
                yield combined;
            }
        }
        if (!this.innerJoin && !yielded) {
            yield template;
        }
    }

    private builder(item: any, side: 'joined' | 'restrictive') {

        // if we're in an inner join, and the chosen strategy
        // has inverted join order, then invert built items
        let template: any;
        let buildItem: (x: any) => any;
        if (side === 'joined') {
            buildItem = x => this.buildItem(x, item);
            template = this.buildItem(null as any, item);
        } else {
            buildItem = x => this.buildItem(item, x);
            template = this.buildItem(item, null as any);
        }
        return { buildItem, template };
    }

    *iterateStrategyItem(item: any, strategy: JoinStrategy, t: _Transaction) {

        const { template, buildItem } = this.builder(item, strategy.iterateSide);

        const joinValue = strategy.onValue.get(item, t);
        let yielded = false;
        if (!nullIsh(joinValue)) {
            // get corresponding right value(s)
            for (const o of strategy.joinIndex.enumerate({
                type: 'eq',
                key: [joinValue],
                t,
            })) {

                // build item
                const item = buildItem(o);

                // check othre predicates (in case the join has an AND statement)
                if (strategy.othersPredicate) {
                    const others = strategy.othersPredicate.get(item, t);
                    if (!others) {
                        continue;
                    }
                }

                // finally, yieldvalue
                yielded = true;
                yield item;
            }
        }

        if (!this.innerJoin && !yielded) {
            yield template;
        }
    }

    buildItem(l: TLeft, r: TRight) {
        const ret = { '>joined': r, '>restrictive': l }
        setId(ret, `join${this.joinId}-${getId(l)}-${getId(r)}`);
        return ret;
    }


    hasItem(value: JoinRaw<TLeft, TRight>): boolean {
        throw new NotSupported('lookups on joins');
    }

    getIndex(forValue: IValue<any>): _IIndex<any> | nil {
        if (this.indexCache.has(forValue)) {
            return this.indexCache.get(forValue);
        }
        // todo: filter using indexes of tables (index propagation)'
        const mapped = this.columnsMappingThisToParent.get(forValue);
        if (!mapped) {
            return null;
        }
        const originIndex = mapped.col.index;
        if (!originIndex) {
            return null;
        }
        const ret = new JoinIndex(this, originIndex, mapped.side);
        this.indexCache.set(forValue, ret);
        return ret;
    }

    explain(e: _Explainer): _SelectExplanation {
        const strategy = chooseStrategy(e.transaction, this.strategies);
        return {
            id: e.idFor(this),
            _: 'join',
            restrictive: this.restrictive.explain(e),
            joined: this.joined.explain(e),
            inner: this.innerJoin,
            on: strategy ? {
                iterate: e.idFor(strategy.iterate),
                iterateSide: strategy.iterateSide,
                joinIndex: strategy.joinIndex.explain(e),
                matches: strategy.onValue.explain(e),
                ...strategy.othersPredicate ? { filtered: true } : {},
            } : {
                    seqScan: this.seqScanExpression.explain(e),
                },
        };
    }
}

export class JoinIndex<T> implements _IIndex<T> {
    constructor(readonly owner: JoinSelection<T>, private base: _IIndex, private side: 'restrictive' | 'joined') {
    }

    get expressions(): IndexExpression[] {
        return this.base.expressions;
    }

    stats(t: _Transaction, key?: IndexKey): Stats | null {
        return null;
    }

    iterateKeys() {
        return null;
    }

    entropy(op: IndexOp): number {
        const strategy = this.chooseStrategy(op.t);
        if (!strategy) {
            // very high entropy (catastophic join)
            return this.base.entropy(op) * this.other.entropy(op.t);
        }
        // todo: multiply that by the mean count per keys in strategy.joinIndex ?
        return this.base.entropy(op);
    }

    eqFirst(rawKey: IndexKey, t: _Transaction): T | null {
        for (const i of this.enumerate({
            type: 'eq',
            key: rawKey,
            t,
        })) {
            return i;
        }
        return null;
    }

    private chooseStrategy(t: _Transaction) {
        const strats = this.owner.strategies.filter(x => x.iterateSide === this.side);
        if (!strats.length) {
            return null;
        }
        return chooseStrategy(t, strats);
    }

    private get other() {
        return this.side === 'joined'
            ? this.owner.restrictive
            : this.owner.joined;
    }

    *enumerate(op: IndexOp): Iterable<T> {
        const strategy = this.chooseStrategy(op.t);
        if (strategy) {
            for (const i of this.base.enumerate(op)) {
                yield* this.owner.iterateStrategyItem(i, strategy, op.t);
            }
        } else {
            this.owner.db.raiseGlobal('catastrophic-join-optimization');
            const all = [...this.other.enumerate(op.t)];

            for (const i of this.base.enumerate(op)) {
                yield* this.owner.iterateCatastrophicItem(i, all, this.side, op.t);
            }
        }
    }


    explain(e: _Explainer): _IndexExplanation {
        const strat = this.chooseStrategy(e.transaction);
        return {
            _: 'indexOnJoin',
            index: this.base.explain(e),
            strategy: strat?.joinIndex?.explain(e) ?? 'catastrophic',
        }
    }
}