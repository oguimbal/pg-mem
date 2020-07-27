import { _Transaction, TR } from './interfaces-private';
import { Map as ImMap, Record, List } from 'immutable';

export type Raw<T> = ImMap<string, T>;
export function getBin<T>(this: void, table: TR<T>, t: _Transaction) {
    return t.getMap<Raw<T>>(table.get('dataId'));
}
export function setBin<T>(this: void, table: TR<T>, t: _Transaction, val: Raw<T>) {
    return t.set(table.get('dataId'), val);
}

export function remapData<T>(this: void, table: TR<T>, t: _Transaction, modify: (newCopy: T) => any) {
    // convert raw data (⚠ must copy the whole thing,
    // because it can throw in the middle of this process !)
    //  => this would result in partially converted tables.
    const converted = getBin(table, t).map(x => {
        const copy = { ...x };
        modify(copy);
        return copy;
    });
    setBin(table, t, converted);
}
