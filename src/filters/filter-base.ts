import { _ISelection, IValue, BuildState, _IIndex } from '../interfaces-private';
import { NotSupported } from '../utils';
import { buildSelection } from './selection';

export abstract class FilterBase<T> implements _ISelection<T> {

    abstract readonly entropy: number;
    abstract enumerate(): Iterable<T>;
    abstract hasItem(value: T): boolean;
    abstract sql(state?: BuildState): string;

    constructor(private _base: _ISelection<T>) {
    }

    get columns(): IValue<any>[] {
        return this._base.columns;
    }


    filter(where: any): _ISelection<any> {
        throw new NotSupported();
    }

    getColumn(column: string): IValue<any> {
        return this._base.getColumn(column);
    }

    select(select: any[] | "*"): _ISelection<any> {
        return buildSelection(this, select);
    }

    getIndex(forValue: IValue): _IIndex<any> {
        return this._base.getIndex(forValue);
    }
}