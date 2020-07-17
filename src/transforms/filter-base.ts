import { _ISelection, IValue, _IIndex } from '../interfaces-private';
import { NotSupported } from '../utils';
import { buildSelection, buildAlias } from './selection';

export abstract class FilterBase<T> implements _ISelection<T> {

    abstract readonly entropy: number;
    abstract enumerate(): Iterable<T>;
    abstract hasItem(value: T): boolean;

    constructor(private _base: _ISelection<T>) {
    }

    get columns(): IValue<any>[] {
        return this._base.columns;
    }


    filter(where: any): _ISelection<any> {
        throw new NotSupported();
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue<any> {
        return this._base.getColumn(column, nullIfNotFound);
    }

    select(select: any[] | "*"): _ISelection<any> {
        return buildSelection(this, select);
    }

    setAlias(alias?: string): _ISelection<any> {
        return buildAlias(this, alias);
    }


    getIndex(forValue: IValue): _IIndex<any> {
        return this._base.getIndex(forValue);
    }
}