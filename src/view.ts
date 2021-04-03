import { QueryError, Reg, _Explainer, _ISchema, _ISelection, _IView, _Transaction } from './interfaces-private';
import { DataSourceBase, FilterBase } from './transforms/transform-base';

export class View extends FilterBase<any> implements _IView {
    get type(): 'view' {
        return 'view';
    }

    private _reg?: Reg;
    get reg(): Reg {
        if (!this._reg) {
            throw new QueryError(`relation "${this.name}" does not exist`);
        }
        return this._reg;
    }

    constructor(readonly ownerSchema: _ISchema, readonly name: string, readonly selection: _ISelection) {
        super(selection);
    }


    enumerate(t: _Transaction): Iterable<any> {
        return this.selection.enumerate(t);
    }

    hasItem(value: any, t: _Transaction): boolean {
        return this.selection.hasItem(value, t);
    }

    explain(e: _Explainer) {
        return this.selection.explain(e);
    }

    stats(t: _Transaction) {
        return this.selection.stats(t);
    }


    register() {
        // once fields registered,
        //  then register the table
        //  (column registrations need it not to be registered yet)
        this._reg = this.ownerSchema._reg_register(this);
        return this;
    }

    drop(t: _Transaction): void {
        throw new Error('Method not implemented.');
    }
}