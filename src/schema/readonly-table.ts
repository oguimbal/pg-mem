import { _ITable, _ISelection, _IQuery, _Transaction, _IIndex, IValue, NotSupported, ReadOnlyError, _Column, CreateColumnDefTyped } from '../interfaces-private';
import { CreateColumnDef } from 'src/parser/syntax/ast';

export abstract class ReadOnlyTable<T = any> implements _ITable {

    abstract get name(): string;
    abstract readonly selection: _ISelection;
    abstract entropy(t: _Transaction): number;
    abstract enumerate(t: _Transaction): Iterable<T>;
    abstract hasItem(value: T, t: _Transaction): boolean;

    hidden = true;

    constructor(readonly schema: _IQuery) {
    }

    get columns(): _Column[] {
        throw new ReadOnlyError('information schema');
    }

    rename(to: string): this {
        throw new ReadOnlyError('information schema');
    }
    update(t: _Transaction, toUpdate: any) {
        throw new ReadOnlyError('information schema');
    }
    addColumn(column: CreateColumnDefTyped | CreateColumnDef): _Column {
        throw new ReadOnlyError('information schema');
    }
    getColumnRef(column: string, nullIfNotFound?: boolean): _Column {
        throw new ReadOnlyError('information schema');
    }
    insert(toInsert: any): void {
        throw new ReadOnlyError('information schema');
    }
    createIndex(): this {
        throw new ReadOnlyError('information schema');
    }

    setReadonly(): this {
        return this;
    }

    getIndex(forValue: IValue): _IIndex<any> {
        return null;
    }

    on(): void {
        throw new NotSupported('subscribing information schema');
    }

}