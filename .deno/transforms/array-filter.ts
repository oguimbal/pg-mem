import { FilterBase } from './transform-base.ts';
import { _ISelection, _Explainer, _SelectExplanation, _Transaction, Stats, Row } from '../interfaces-private.ts';

export class ArrayFilter extends FilterBase {

    get index() {
        return null;
    }

    entropy() {
        return this.rows.length;
    }

    hasItem(raw: Row): boolean {
        return this.rows.includes(raw);
    }

    getIndex() {
        return null;
    }

    constructor(fromTable: _ISelection, public rows: Row[]) {
        super(fromTable);
    }

    enumerate(): Iterable<Row> {
        return this.rows;
    }

    stats(t: _Transaction): Stats | null {
        return {
            count: this.rows.length,
        };
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'constantSet',
            rawArrayLen: this.rows.length,
        }
    }
}