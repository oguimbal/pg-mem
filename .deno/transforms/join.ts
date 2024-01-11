import { _ISelection, IValue, _IIndex, _IDb, setId, getId, _Transaction, _ISchema, _SelectExplanation, _Explainer, IndexExpression, IndexOp, IndexKey, _IndexExplanation, Stats, _IAlias, TR, _IStatement } from '../interfaces-private.ts';
import { buildBinaryValue, buildValue, uncache } from '../parser/expression-builder.ts';
import { QueryError, ColumnNotFound, NotSupported, nil, DataType } from '../interfaces.ts';
import { DataSourceBase, TransformBase } from './transform-base.ts';
import { Expr, ExprRef, JoinClause, Name, SelectedColumn } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { colToStr, nullIsh, SRecord } from '../utils.ts';
import { Types } from '../datatypes/index.ts';
import { SELECT_ALL } from '../execution/clean-results.ts';
import { CustomAlias, Selection } from './selection.ts';
import { withSelection, buildCtx } from '../parser/context.ts';

let jCnt = 0;

interface JoinRaw<TLeft, TRight> {
    '>restrictive': TLeft;
    '>joined': TRight;
}
interface JoinStrategy {
    iterate: _ISelection;
    iterateSide: 'joined' | 'restrictive';
    joinIndex: _IIndex<any>;
    onValue: IValue;
    othersPredicate?: IValue<any>;
}

interface Equality {
    left: IValue;
    right: IValue;
    eq: IValue;
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

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    private _columns: IValue<any>[] = [];
    private seqScanExpression!: IValue<any>;
    private joinId: number;
    private columnsMappingParentToThis = new Map<IValue, IValue>();
    // mapping of the left table columns mapped to the actual their inner value
    private indexOnRestrictingTableByValue = new Map<IValue, _IIndex>();
    private indexCache = new Map<IValue, _IIndex>();
    strategies: JoinStrategy[] = [];
    private building = false;
    private ignoreDupes?: Set<IValue>;
    private mergeSelect?: Selection;


    isOriginOf(a: IValue<any>): boolean {
        return this.joined.isOriginOf(a) || this.restrictive.isOriginOf(a);
    }

    get columns(): IValue<any>[] {
        return this._columns;
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

    constructor(readonly restrictive: _ISelection<TLeft>
        , readonly joined: _ISelection<TRight>
        , on: JoinClause
        , readonly innerJoin: boolean) {
        super(buildCtx().schema);


        this.joinId = jCnt++;
        for (const c of this.restrictive.listSelectableIdentities()) {
            const nc = c.setWrapper(this, x => (x as any)['>restrictive']);
            this.columnsMappingParentToThis.set(c, nc);
            if (c.type.primary === DataType.record) {
                continue;
            }
            this._columns.push(nc);
            if (c.index) {
                this.indexOnRestrictingTableByValue.set(nc, c.index);
            }
        }
        for (const c of this.joined.listSelectableIdentities()) {
            const nc = c.setWrapper(this, x => (x as any)['>joined']);
            this.columnsMappingParentToThis.set(c, nc);
            if (c.type.primary === DataType.record) {
                continue;
            }
            this._columns.push(nc);
        }

        withSelection(this, () => {
            if (on.on) {
                this.fetchOnStrategies(on.on);
            } else if (on.using?.length) {
                this.fetchUsingStrategies(on.using);
            } else {
                throw new Error('Unspecified join ON clause');
            }
        });
    }

    private wrap(v: IValue) {
        const ret = this.columnsMappingParentToThis.get(v);
        if (!ret) {
            throw new Error('Corrupted join (unknown column)');
        }
        return ret;
    }

    listSelectableIdentities(): Iterable<IValue> {
        return this.columnsMappingParentToThis.values();
    }

    private fetchOnStrategies(_on: Expr) {
        // build equalities eligible to a strategy
        const ands: Equality[] = [];
        const others: IValue[] = [];
        for (const on of extractAnds(_on)) {
            if (on.type !== 'binary' || on.op !== '=') {
                // join 'ON' clause not compatible with an indexed strategy
                others.push(buildValue(on));
                continue;
            }
            this.building = true;
            const left = buildValue(on.left);
            const right = buildValue(on.right);
            this.building = false;
            // necessary because of the 'this.building' hack
            uncache(this);
            ands.push({
                left,
                right,
                eq: buildValue(on),
            });
        }

        // compute strategies
        this.fetchAndStrategies(ands, others);


        // build seq-scan expression
        this.seqScanExpression = buildValue(_on).cast(Types.bool);
    }

    private fetchUsingStrategies(_using: Name[]) {
        // build equalities eligible to a strategy
        const ands = _using.map<Equality>(n => {
            const left = this.restrictive.getColumn(n.name);
            const right = this.joined.getColumn(n.name);
            return {
                left,
                right,
                eq: buildBinaryValue(
                    this.wrap(left)
                    , '='
                    , this.wrap(right))
            }
        });
        this.ignoreDupes = new Set(ands.map(x => this.wrap(x.left)));

        // compute strategies
        this.fetchAndStrategies(ands, []);

        // build seq-scan expression
        this.seqScanExpression = ands.slice(1)
            .reduce((a, b) => buildBinaryValue(a, 'AND', b.eq), ands[0].eq);
    }

    private fetchAndStrategies(ands: Equality[], otherPredicates: IValue[]) {

        for (let i = 0; i < ands.length; i++) {
            const { left, right } = ands[i];
            const strats = [...this.fetchEqStrategyOn(left, right)];
            if (!strats.length) {
                continue;
            }
            const others = [
                ...ands.slice(0, i).map(x => x.eq),
                ...ands.slice(i + 1).map(x => x.eq),
                ...otherPredicates
            ];
            if (others.length) {
                const and = others.slice(1)
                    .reduce<IValue>((v, c) => buildBinaryValue(c, 'AND', v)
                        , others[0]);
                for (const s of strats) {
                    s.othersPredicate = and;
                }
            }
            this.strategies.push(...strats);
        }
    }

    private *fetchEqStrategyOn(a: IValue, b: IValue): Iterable<JoinStrategy> {
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

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue<any> | nil {
        let onLeft = this.restrictive.getColumn(column, true);
        let onRight = this.joined.getColumn(column, true);
        if (!onLeft && !onRight) {
            if (nullIfNotFound) {
                return null;
            }
            throw new ColumnNotFound(colToStr(column));
        }
        if (!!onLeft && !!onRight) {
            throw new QueryError(`column reference "${colToStr(column)}" is ambiguous`);
        }
        const on = onLeft ?? onRight;
        if (this.building) {
            return on;
        }
        const mapped = this.columnsMappingParentToThis.get(on!);
        if (mapped) {
            return mapped;
        }
        throw new Error('Corrupted join');
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


    selectAlias(alias: string): _IAlias | nil {
        let onLeft = this.restrictive.selectAlias(alias);
        let onRight = this.joined.selectAlias(alias);
        if (!onLeft && !onRight) {
            return null;
        }
        if (!!onLeft && !!onRight) {
            throw new QueryError(`alias "${alias}" is ambiguous`);
        }
        return new JoinMapAlias(this, onLeft ?? onRight!, onLeft ? '>restrictive' : '>joined');
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
        const ret = {
            '>joined': r,
            '>restrictive': l,
            [SELECT_ALL]: () => this.merge(ret),
        }
        setId(ret, `join${this.joinId}-${getId(l)}-${getId(r)}`);
        return ret;
    }

    private merge(item: any) {
        if (!this.mergeSelect) {
            let sel = this.columns.map<CustomAlias>(val => ({ val }));
            if (this.ignoreDupes) {
                sel = sel.filter(t => !this.ignoreDupes?.has(t.val));
            }
            this.mergeSelect = new Selection(this, sel);
        }

        // nb: second argument is null... this is a hack : we KNOW it wont use the transaction.
        const ret = this.mergeSelect.build(item, Symbol('hack') as any);
        return ret;
    }

    hasItem(value: JoinRaw<TLeft, TRight>): boolean {
        throw new NotSupported('lookups on joins');
    }

    getIndex(forValue: IValue<any>): _IIndex<any> | nil {
        if (this.indexCache.has(forValue)) {
            return this.indexCache.get(forValue);
        }
        // check if the restrictive table (left part) has an index
        // cant use indexes on joined table (right part), see #306 unit test
        // todo: filter using indexes of tables (index propagation)'
        const mapped = this.indexOnRestrictingTableByValue.get(forValue);
        if (!mapped) {
            return null;
        }
        const ret = new JoinIndex(this, mapped);
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


class JoinMapAlias implements _IAlias {


    constructor(private owner: JoinSelection, private target: _IAlias, private map: string) {
    }

    *listColumns(): Iterable<IValue<any>> {
        for (const c of this.target.listColumns()) {
            yield c.setWrapper(this.owner, x => (x as any)[this.map]);
        }
    }
}

export class JoinIndex<T> implements _IIndex<T> {
    constructor(readonly owner: JoinSelection<T>, private base: _IIndex) {
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
            return this.base.entropy(op) * this.owner.joined.entropy(op.t);
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
        const strats = this.owner.strategies.filter(x => x.iterateSide === 'restrictive');
        if (!strats.length) {
            return null;
        }
        return chooseStrategy(t, strats);
    }

    *enumerate(op: IndexOp): Iterable<T> {
        const strategy = this.chooseStrategy(op.t);
        if (strategy) {
            for (const i of this.base.enumerate(op)) {
                yield* this.owner.iterateStrategyItem(i, strategy, op.t);
            }
        } else {
            // not sure we can reach that...
            this.owner.db.raiseGlobal('catastrophic-join-optimization');
            const all = [...this.owner.joined.enumerate(op.t)];

            for (const i of this.base.enumerate(op)) {
                yield* this.owner.iterateCatastrophicItem(i, all, 'restrictive', op.t);
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