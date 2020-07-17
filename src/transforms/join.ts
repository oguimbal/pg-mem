import { _ISelection, IValue, _IIndex } from '../interfaces-private';
import { buildValue } from '../predicate';
import { QueryError, ColumnNotFound } from '../interfaces';
import { buildAlias } from './selection';
import { relativeTimeRounding } from 'moment';
import { buildAndFilter } from './and-filter';

export class JoinSelection implements _ISelection {
    columns: IValue<any>[];
    private leftExpression: IValue<any>;
    private rightExpression: IValue<any>;
    private seqScanExpression: IValue<any>;

    get entropy(): number {
        return this.left.entropy;
    }

    constructor(private left: _ISelection, private right: _ISelection, on: any) {

        // only support indexed joins on binary expressions
        if (on.type === 'binary_expr') {
            const a = buildValue(this, on.left);
            const b = buildValue(this, on.right);
            if (b.index && b.origin === right && a.origin === left) {
                // right part of binary expression is an index on the joined table
                this.leftExpression = a;
                this.rightExpression = b;
            } else if (a.index && a.origin === right && b.origin === left) {
                // left part of binary expression is an index on the joined table
                this.leftExpression = b;
                this.rightExpression = a;
            }
        }

        this.seqScanExpression = buildValue(this, on);
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

    setAlias(alias?: string): _ISelection<any> {
        return buildAlias(this, alias);
    }

    filter(where: any): _ISelection<any> {
        return buildFile
    }

    select(select: any[] | "*"): _ISelection<any> {

    }


    enumerate(): Iterable<any> {

    }

    hasItem(value: any): boolean {

    }

    getIndex(forValue: IValue<any>): _IIndex<any> { // istanbul ignore next
        return null;
    }
}