import { IValue, _IIndex, _ITable } from './interfaces-private';
import createTree from 'functional-red-black-tree';

type IndexKey = any[];
// https://www.npmjs.com/package/functional-red-black-tree
interface BTree<T> {
    remove(key: IndexKey): BTree<T>;
    find(key: IndexKey): BIterator<T>;
    get(key: IndexKey): T;
    insert(key: IndexKey, value: T): BTree<T>;
    readonly length: number;
}

interface BIterator<T> {
    readonly key: IndexKey;
    readonly value: T;
    // tree;
    // index;
    readonly valid: boolean;
    clone(): BIterator<T>;
    remove(): BTree<T>;
    // update(value: PrimaryKey): BTree;
    next(): void;
    prev(): void;
    readonly hasNext: boolean;
    readonly hasPrev: boolean;
}


export class BIndex<T = any> implements _IIndex<T> {


    private asBinary: BTree<Set<T>>;
    size = 0;

    get entropy() {
        if (!this.asBinary.length) {
            return 0;
        }
        return this.size / this.asBinary.length;
    }


    constructor(readonly expressions: IValue[]
        , readonly onTable: _ITable<T>) {
        this.asBinary = createTree((a: any, b: any) => {
            return this.compare(a, b);
        });
        this.asBinary = createTree((a: any, b: any) => {
            return this.compare(a, b);
        });
    }

    private compare(a: any, b: any) {
        for (let i = 0; i < this.expressions.length; i++) {
            const k = this.expressions[i];
            if (k.equals(a[i], b[i])) {
                continue;
            }
            return k.gt(a, b)
                ? 1
                : -1;
        }
        return 0;
    }

    private buildKey(raw: any) {
        return this.expressions.map(k => k.get(raw));
    }

    hasItem(raw: any) {
        if (!this.asBinary) {
            return false;
        }
        const key = this.buildKey(raw);
        const id = this.asBinary.get(key);
        return id !== undefined;
    }

    add(raw: T) {
        const key = this.buildKey(raw);
        let got = this.asBinary.get(key);
        if (!got) {
            this.asBinary = this.asBinary.insert(key, got = new Set());
        }
        if (got.has(raw)) {
            return;
        }
        this.size++;
        got.add(raw);
    }

    delete(raw: any) {
        const key = this.buildKey(raw);
        let got = this.asBinary.get(key);
        if (!got) {
            return;
        }
        if (!got.has(raw)) {
            return;
        }
        this.size--;
        got.delete(raw);
        if (!got.size) {
            this.asBinary = this.asBinary.remove(key);
        }
    }


    *eq(key: IndexKey): Iterable<T> {
        const it = this.asBinary.find(key);
        while (it.valid && this.compare(it.key, key) === 0) {
            yield* it.value.values();
            it.next();
        }
    }

    *gt(key: IndexKey): Iterable<T> {
        const it = this.asBinary.find(key);
        while (it.valid && this.compare(it.key, key) === 0) {
            it.next();
        }
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }

    *lt(key: IndexKey): Iterable<T> {
        const it = this.asBinary.find(key);
        while (it.valid && this.compare(it.key, key) === 0) {
            it.prev();
        }
        while (it.valid) {
            yield* it.value.values();
            it.prev();
        }
    }

    *ge(key: IndexKey): Iterable<T> {
        const it = this.asBinary.find(key);
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }

    *le(key: IndexKey): Iterable<T> {
        const it = this.asBinary.find(key);
        while (it.valid) {
            yield* it.value.values();
            it.prev();
        }
    }
}