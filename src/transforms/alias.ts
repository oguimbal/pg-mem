import { TransformBase, FilterBase } from './transform-base';
import { _Transaction, IValue, _Explainer, _ISelection, _SelectExplanation, QueryError } from '../interfaces-private';

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

    get debugId() {
        return this.base.debugId;
    }

    constructor(sel: _ISelection, public name: string) {
        super(sel);
    }

    get columns(): ReadonlyArray<IValue<any>> {
        return this.base.columns;
    }

    enumerate(t: _Transaction): Iterable<T> {
        return this.base.enumerate(t);
    }

    hasItem(value: T, t: _Transaction): boolean {
        return this.base.hasItem(value, t);
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue {
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
        return this.base.getColumn(column, nullIfNotFound);
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

    getIndex(forValue: IValue) {
        return this.base.getIndex(forValue);
    }

}