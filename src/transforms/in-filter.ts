import { _ISelection, IValue, _IIndex, _ITable, getId, _Transaction, _Explainer, _SelectExplanation, Stats, Row } from '../interfaces-private';
import { FilterBase } from './transform-base';
import { DataType, CastError, QueryError } from '../interfaces';
import { nullIsh } from '../utils';

export class InFilter extends FilterBase {


    private index: _IIndex;

    entropy(t: _Transaction) {
        let ret = 0;
        for (const a of this.elts) {
            ret += this.index.entropy({
                type: 'eq',
                key: [a],
                t,
            });
        }
        return ret;
    }

    hasItem(item: Row, t: _Transaction) {
        const val = this.onValue.get(item, t);
        return !nullIsh(val)
            && this.elts.some(x => this.onValue.type.equals(x, val));
    }

    constructor(private onValue: IValue
        , private elts: any[]) {
        super(onValue.origin!);
        this.index = onValue.index!;
        if (this.index.expressions.length !== 1) {
            throw new Error('Only supports IN with signle expressions index');
        }
        if (!Array.isArray(elts)) {
            throw new QueryError('Cannot iterate element list');
        }
    }

    stats(t: _Transaction): Stats | null {
        const elts = this.elts.map(x => this.index.stats(t, [x]));
        if (elts.some(x => !x)) {
            return null;
        }
        // compute from elements
        const ret: Stats = {
            count: 0,
        };
        for (const i of elts) {
            ret.count += i!.count;
        }
        return ret;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        for (const a of this.elts) {
            yield* this.index.enumerate({
                type: 'eq',
                key: [a],
                t,
            });
        }
    }


    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'eq',
            entropy: this.entropy(e.transaction),
            on: this.index.explain(e),
        };
    }
}