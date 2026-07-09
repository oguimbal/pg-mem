import { QueryError, Reg, _Explainer, _ISchema, _ISelection, _IView, _Transaction } from '../interfaces-private';
import { DataSourceBase, FilterBase } from '../transforms/transform-base';
import { TableTriggers, emptyTriggers, Trigger } from '../execution/triggers';

export class View extends FilterBase implements _IView {
    get type(): 'view' {
        return 'view';
    }

    // INSTEAD OF triggers (a view has no rows of its own to mutate)
    readonly triggers: TableTriggers = emptyTriggers();

    createTrigger(trigger: Trigger): void {
        if (this.triggers.triggers.some(t => t.name === trigger.name)) {
            throw new QueryError(`trigger "${trigger.name}" for relation "${this.name}" already exists`, '42710');
        }
        this.triggers.triggers.push(trigger);
    }

    dropTrigger(name: string, ifExists: boolean): void {
        const idx = this.triggers.triggers.findIndex(t => t.name === name);
        if (idx < 0) {
            if (ifExists) { return; }
            throw new QueryError(`trigger "${name}" for view "${this.name}" does not exist`, '42704');
        }
        this.triggers.triggers.splice(idx, 1);
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