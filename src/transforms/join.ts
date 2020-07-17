import { _ISelection, IValue, _IIndex, _IDb, setId, getId } from '../interfaces-private';
import { buildValue } from '../predicate';
import { QueryError, ColumnNotFound, DataType } from '../interfaces';
import { DataSourceBase } from './transform-base';
import { buildColumnIds } from '../utils';

let jCnt = 0;

export class JoinSelection<TLeft = any, TRight = any> extends DataSourceBase<TLeft & TRight> {


    private _columns: IValue<any>[] = [];
    private leftExpression: IValue<any>;
    private indexedRight: _IIndex<any>;
    private seqScanExpression: IValue<any>;
    private columnIds: string[];
    private joinId: number;


    get columns(): IValue<any>[] {
        return this._columns;
    }

    get leftColumns(): IValue<any>[] {
        return this.left.columns;
    }

    get rightColumns(): IValue<any>[] {
        return this.right.columns;
    }

    get entropy(): number {
        return this.left.entropy;
    }

    constructor(private db: _IDb, private left: _ISelection<TLeft>, private right: _ISelection<TRight>, on: any) {
        super();

        this.joinId = jCnt++;
        this._columns = [
            ...this.leftColumns
            , ...this.rightColumns
        ];
        this.columnIds = buildColumnIds(this.columns);

        // only support indexed joins on binary expressions
        // todo: multiple columns indexes join
        if (on.type === 'binary_expr') {
            const a = buildValue(this, on.left);
            const b = buildValue(this, on.right);
            if (b.index && b.origin === right && a.origin === left) {
                // right part of binary expression is an index on the joined table
                this.leftExpression = a;
                this.indexedRight = b.index;
            } else if (a.index && a.origin === right && b.origin === left) {
                // left part of binary expression is an index on the joined table
                this.leftExpression = b;
                this.indexedRight = a.index;
            }
        }

        this.seqScanExpression = buildValue(this, on).convert(DataType.bool);
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
        const onLeft = this.left.getColumn(column, true);
        const onRight = this.right.getColumn(column, true);
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

    hasItem(value: TLeft & TRight): boolean {
        throw new Error('todo');
    }

    getIndex(forValue: IValue<any>): _IIndex<any> { // istanbul ignore next
        throw new Error('todo: filter using indexes of tables (index propagation)');
        return null;
    }



    *enumerate(): Iterable<any> {
        // todo: filter & indexes
        for (const l of this.left.enumerate()) {
            let r: TRight;

            // find the right value using index
            if (this.indexedRight) {
                const joinValue = this.leftExpression.get(l);
                // get corresponding right value
                r = this.indexedRight.eqFirst([joinValue]);
            } else {
                // perform a seq scan
                this.db.raiseGlobal('catastrophic-join-optimization');
                for (const cr of this.right.enumerate()) {
                    const combined = this.buildItem(l, cr);
                    const result = this.seqScanExpression.get(combined);
                    if (result) {
                        r = cr;
                        break;
                    }
                }
            }

            yield this.buildItem(l, r);
        }
    }

    private buildItem(l: TLeft, r: TRight) {
        const ret = {};
        setId(ret, `join${this.joinId}-${getId(l)}-${getId(r)}`);

        let i = 0;

        // build left part result
        for (; i < this.leftColumns.length; i++) {
            const col = this.leftColumns[i];
            ret[this.columnIds[i]] = col.get(l) ?? null;
        }

        // build right part result
        const rnul = r === null || r === undefined;
        for (; i < this.columns.length; i++) {
            const col = this.columns[i];
            ret[this.columnIds[i]] = rnul ? null : (col.get(r) ?? null);
        }
        return ret;
    }
}
