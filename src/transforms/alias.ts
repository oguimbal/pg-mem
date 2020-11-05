import { TransformBase, FilterBase } from './transform-base';
import { _Transaction, IValue, _Explainer, _ISelection, _SelectExplanation, QueryError, Stats, nil } from '../interfaces-private';

export function buildAlias(on: _ISelection, alias?: string): _ISelection<any> {
    if (!alias) {
        return on as any;
    }
    alias = alias.toLowerCase();
    if (on instanceof Alias && on.name === alias) {
        return on;
    }
    return new Alias(on, alias);
}

export class Alias<T> extends TransformBase<T>{

    private oldToThis = new Map<IValue, IValue>();
    private thisToOld = new Map<IValue, IValue>();
    private _columns: IValue<any>[] | null = null;

    constructor(sel: _ISelection, public name: string) {
        super(sel);
    }

    rebuild() {
        this._columns = null;
        this.oldToThis.clear();
        this.thisToOld.clear();
    }

    get debugId() {
        return this.base.debugId;
    }

    get columns(): ReadonlyArray<IValue<any>> {
        this.init();
        return this._columns!;
    }
    init() {
        if (this._columns) {
            return;
        }
        this._columns = this.base.columns.map(x => {
            const ret = x.setOrigin(this);
            this.oldToThis.set(x, ret);
            this.thisToOld.set(ret, x);
            return ret;
        });
    }

    stats(t: _Transaction): Stats | null {
        return this.base.stats(t);
    }

    enumerate(t: _Transaction): Iterable<T> {
        return this.base.enumerate(t);
    }

    hasItem(value: T, t: _Transaction): boolean {
        return this.base.hasItem(value, t);
    }

    getColumn(column: string): IValue;
    getColumn(column: string, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string, nullIfNotFound?: boolean): IValue | nil {
        const exec = /^([^.]+)\.(.+)$/.exec(column);
        if (exec) {
            if (exec[1].toLowerCase() !== this.name) {
                if (nullIfNotFound) {
                    return null;
                }
                throw new QueryError(`Alias '${exec[1]}' not found`)
            }
            column = exec[2];
        }
        const got = this.base.getColumn(column, nullIfNotFound);
        if (!got) {
            return got;
        }
        this.init();
        const ret = this.oldToThis.get(got);
        if (!ret) {
            throw new Error('Corrupted alias');
        }
        return ret;
    }

    explain(e: _Explainer): _SelectExplanation {
        // no need to explain an alias... it does nothing.
        return this.base.explain(e);
        // return {
        //     id: e.idFor(this),
        //     type: 'alias',
        //     alias: this.as,
        //     of: this.base.explain(e),
        // };
    }

    getIndex(...forValue: IValue[]) {
        return this.base.getIndex(...forValue.map(v => this.thisToOld.get(v) ?? v));
    }

}