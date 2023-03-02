import { _IConstraint, _IIndex, _ITable, _Transaction } from '../interfaces-private.ts';

export class IndexConstraint implements _IConstraint {

    constructor(readonly name: string, readonly index: _IIndex, private table: _ITable) {
    }

    uninstall(t: _Transaction): void {
        this.table.dropIndex(t, this.name);
    }
}