import { _ISelection, IValue, _IIndex, _IDb, setId, getId, _Transaction, _ISchema, _SelectExplanation, _Explainer, IndexExpression, IndexOp, IndexKey, _IndexExplanation } from '../interfaces-private';
import { buildValue } from '../predicate';
import { QueryError, ColumnNotFound, DataType, NotSupported } from '../interfaces';
import { DataSourceBase } from './transform-base';
import { Expr } from '../parser/syntax/ast';

let jCnt = 0;

interface JoinRaw<TLeft, TRight> {
    '>left': TLeft;
    '>right': TRight;
}
export class JoinSelection<TLeft = any, TRight = any> extends DataSourceBase<JoinRaw<TLeft, TRight>> {


    private _columns: IValue<any>[] = [];
    private leftExpression: IValue<any>;
    private indexedRight: _IIndex<any>;
    private seqScanExpression: IValue<any>;
    private joinId: number;
    private columnsMapping = new Map<IValue, IValue>();
    private indexCache = new Map<IValue, _IIndex>();

    get columns(): IValue<any>[] {
        return this._columns;
    }

    private get leftColumns() {
        return this.left.columns
    }

    private get rightColumns() {
        return this.right.columns
    }

    entropy(t: _Transaction): number {
        return this.left.entropy(t);
    }

    constructor(db: _ISchema
        , readonly left: _ISelection<TLeft>
        , readonly right: _ISelection<TRight>
        , on: Expr
        , private innerJoin: boolean) {
        super(db);

        this.joinId = jCnt++;
        for (const c of this.leftColumns) {
            const nc = c.setWrapper(this, x => x['>left']);
            this._columns.push(nc);
            this.columnsMapping.set(nc, c);
        }
        for (const c of this.rightColumns) {
            const nc = c.setWrapper(this, x => x['>right']);
            this._columns.push(nc);
            this.columnsMapping.set(nc, c);
        }

        // only support indexed joins on binary expressions
        // todo: multiple columns indexes join
        if (on.type === 'binary') {
            const a = buildValue(this, on.left);
            const b = buildValue(this, on.right);

            // const aIndex = a.wrappedOrigin?.getIndex()
            if (b.index && b.index.expressions.length === 1 && b && a.origin === left && b.origin === right) {
                // right part of binary expression is an index on the joined table
                this.leftExpression = a;
                this.indexedRight = b.index;
            } else if (a.index && a.index.expressions.length === 1 && a.origin === right && b.origin === left) {
                // left part of binary expression is an index on the joined table
                this.leftExpression = b;
                this.indexedRight = a.index;
            }
        }

        this.seqScanExpression = buildValue(this, on).convert(DataType.bool);
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
        const onLeft = this.left.getColumn(column, true)?.setWrapper(this, x => x['>left']);
        const onRight = this.right.getColumn(column, true)?.setWrapper(this, x => x['>right']);
        if (!onLeft && !onRight) {
            if (nullIfNotFound) {
                return null;
            }
            throw new ColumnNotFound(column);
        }
        if (!!onLeft && !!onRight) {
            throw new QueryError(`column reference "${column}" is ambiguous`);
        }
        return onLeft ?? onRight;
    }


    *enumerate(t: _Transaction): Iterable<any> {

        if (this.indexedRight) {
            // find the right value using index
            for (const l of this.left.enumerate(t)) {
                const joinValue = this.leftExpression.get(this.buildItem(l, null), t);
                // get corresponding right value
                let yielded = false;
                for (const r of this.indexedRight.enumerate({
                    type: 'eq',
                    key: [joinValue],
                    t,
                })) {
                    yielded = true;
                    yield this.buildItem(l, r);
                }

                if (!this.innerJoin && !yielded) {
                    yield this.buildItem(l, null);
                }
            }
        } else {
            // perform a seq scan
            this.schema.db.raiseGlobal('catastrophic-join-optimization');
            const allRight = [...this.right.enumerate(t)];
            for (const l of this.left.enumerate(t)) {
                let yielded = false;
                for (const cr of allRight) {
                    const combined = this.buildItem(l, cr);
                    const result = this.seqScanExpression.get(combined, t);
                    if (result) {
                        yielded = true;
                        yield combined;
                        break;
                    }
                }
                if (!this.innerJoin && !yielded) {
                    yield this.buildItem(l, null);
                }
            }
        }
    }

    buildItem(l: TLeft, r: TRight) {
        const ret = { '>right': r, '>left': l }
        setId(ret, `join${this.joinId}-${getId(l)}-${getId(r)}`);
        return ret;
    }


    hasItem(value: JoinRaw<TLeft, TRight>): boolean {
        throw new NotSupported('lookups on joins');
    }

    getIndex(forValue: IValue<any>): _IIndex<any> {
        if (this.indexCache.has(forValue)) {
            return this.indexCache.get(forValue);
        }
        // todo: filter using indexes of tables (index propagation)'
        const mapped = this.columnsMapping.get(forValue);
        const originIndex = mapped.index;
        const ret = new JoinIndex(this, originIndex);
        this.indexCache.set(forValue, ret);
        return ret;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'join',
            join: this.left.explain(e),
            with: this.right.explain(e),
            inner: this.innerJoin,
            on: this.indexedRight ? {
                index: this.indexedRight.explain(e),
                matches: this.leftExpression.explain(e),
            } : {
                    seqScan: this.seqScanExpression.explain(e),
                },
        };
    }
}

export class JoinIndex<T> implements _IIndex<T> {
    constructor(readonly owner: JoinSelection<T>, private base: _IIndex) {
    }

    get expressions(): IndexExpression[] {
        return this.base.expressions;
    }


    entropy(op: IndexOp): number {
        // same as source
        return this.base.entropy(op);
    }

    eqFirst(rawKey: IndexKey, t: _Transaction): T {
        for (const i of this.enumerate({
            type: 'eq',
            key: rawKey,
            t,
        })) {
            return i;
        }
    }

    *enumerate(op: IndexOp): Iterable<T> {
        for (const i of this.base.enumerate(op)) {
            yield this.owner.buildItem(i, op.t);
        }
    }


    explain(e: _Explainer): _IndexExplanation {
        return {
            _: 'joinIndex',
            of: this.base.explain(e),
        }
    }
}