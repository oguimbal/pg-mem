import { IValue, _IIndex, _ITable, getId, IndexKey, CreateIndexColDef, _Transaction } from './interfaces-private';
import createTree from 'functional-red-black-tree';
import { QueryError } from './interfaces';


// https://www.npmjs.com/package/functional-red-black-tree
interface BTree<T> {
    readonly keys: Iterable<IndexKey>;
    readonly values: Iterable<IndexKey>;
    readonly length: number;
    get(key: IndexKey): T;
    insert(key: IndexKey, value: T): BTree<T>;
    remove(key: IndexKey): BTree<T>;
    find(key: IndexKey): BIterator<T>;
    /** Find the first item in the tree whose key is >= key */
    ge(key: IndexKey): BIterator<T>;
    /** Finds the first item in the tree whose key is > key */
    gt(key: IndexKey): BIterator<T>;
    /** Finds the last item in the tree whose key is < key */
    lt(key: IndexKey): BIterator<T>;
    /** Finds the last item in the tree whose key is <= key */
    le(key: IndexKey): BIterator<T>;

    at(pos: number): BIterator<T>;

    readonly begin: BIterator<T>;
    readonly end: BIterator<T>;
    /**  If a truthy value is returned from the visitor, then iteration is stopped. */
    forEach(fn: (key: IndexKey, value: T) => boolean, low?: number, high?: number);
    // root;
}

interface BIterator<T> {
    readonly key: IndexKey;
    readonly value: T;
    // tree;
    readonly index;
    readonly valid: boolean;
    clone(): BIterator<T>;
    remove(): BTree<T>;
    // update(value: PrimaryKey): BTree;
    next(): void;
    prev(): void;
    readonly hasNext: boolean;
    readonly hasPrev: boolean;
}

type RawTree<T> = BTree<Map<string, T>>;;
export class BIndex<T = any> implements _IIndex<T> {


    // private asBinary: RawTree;
    private byId = new Set<string>();
    expressions: IValue[];
    private id = Symbol();

    size(t: _Transaction) {
        return this.byId.size;
    }

    entropy(t: _Transaction) {
        const asBinary = t.get<RawTree<T>>(this.id);
        if (!asBinary.length) {
            return 0;
        }
        return this.size(t) / asBinary.length;
    }


    constructor(t: _Transaction
        , private cols: CreateIndexColDef[]
        , readonly onTable: _ITable<T>
        , readonly hash: string
        , public indexName: string
        , private unique: boolean
        , private notNull: boolean) {
        const asBinary = createTree((a: any, b: any) => {
            return this.compare(a, b);
        });
        t.set(this.id, asBinary);
        this.expressions = cols.map(x => x.value);
    }

    compare(_a: any, _b: any) {
        for (let i = 0; i < this.expressions.length; i++) {
            const k = this.cols[i];
            const a = _a[i];
            const b = _b[i];
            if (a === null || b === null) {
                if (a === b) {
                    continue;
                }
                return (a === null
                    ? -1
                    : 1) * (k.nullsLast ? 1 : -1);
            }
            if (k.value.type.equals(a, b)) {
                continue;
            }
            return (k.value.type.gt(a, b)
                ? 1
                : -1)// * (k.desc ? -1 : 1);
        }
        return 0;
    }

    private buildKey(raw: any, t: _Transaction) {
        return this.expressions.map(k => k.get(raw, t));
    }

    hasItem(raw: any) {
        const key = getId(raw);
        return this.byId.has(key);
    }

    private bin(t: _Transaction) {
        return t.get<RawTree<T>>(this.id);
    }
    private setBin(t: _Transaction, val: RawTree<T>) {
        return t.set(this.id, val);
    }

    hasKey(key: IndexKey[], t: _Transaction): boolean {
        const it = this.bin(t).find(key);
        return it.valid;
    }

    add(raw: T, t: _Transaction) {
        const id = getId(raw);
        if (this.byId.has(id)) {
            return;
        }
        const key = this.buildKey(raw, t);
        if (this.notNull && key.some(x => x === null || x === undefined)) {
            throw new QueryError('Cannot add a null record in index ' + this.indexName);
        }
        if (this.unique && this.hasKey(key, t)) {
            throw new QueryError('Unique constraint violated while adding a record to index ' + this.indexName);
        }
        let bin = this.bin(t);
        let got = bin.get(key);
        if (!got) {
            bin = this.setBin(t, bin.insert(key, got = new Map()));
        }
        if (got.has(id)) {
            return;
        }
        got.set(id, raw);
        this.byId.add(id);
    }

    delete(raw: any, t: _Transaction) {
        const key = this.buildKey(raw, t);
        let bin = this.bin(t);
        let got = bin.get(key);
        if (!got) {
            return;
        }
        const id = getId(raw);
        if (!got.has(id)) {
            return;
        }
        this.byId.delete(id);
        got.delete(id);
        if (!got.size) {
            bin = this.setBin(t, bin.remove(key));
        }
    }

    eqFirst(rawKey: IndexKey, t: _Transaction): T {
        for (const r of this.eq(rawKey, t)) {
            return r;
        }
    }


    *eq(key: IndexKey, t: _Transaction): Iterable<T> {
        const it = this.bin(t).find(key);
        while (it.valid && this.compare(it.key, key) === 0) {
            yield* it.value.values();
            it.next();
        }
    }

    *nin(rawKey: IndexKey[], t: _Transaction): Iterable<T> {
        rawKey.sort((a, b) => this.compare(a, b));
        const kit = rawKey[Symbol.iterator]();
        let cur = kit.next();
        const bin = this.bin(t);
        let it = bin.begin;
        while (!cur.done) {
            // yield previous
            while (it.valid && this.compare(it.key, cur.value) < 0) {
                yield* it.value.values();
                it.next();
            }
            // skip equals
            if (this.compare(it.key, cur.value) === 0) {
                it = bin.gt(cur.value);
            }
            cur = kit.next();
        }

        // finish
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }


    *neq(key: IndexKey, t: _Transaction): Iterable<T> {
        // yield before
        const bin = this.bin(t);
        let it = bin.begin;
        while (it.valid && this.compare(it.key, key) < 0) {
            yield* it.value.values();
            it.next();
        }
        // yield after
        it = bin.gt(key);
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }

    *gt(key: IndexKey, t: _Transaction): Iterable<T> {
        const it = this.bin(t).gt(key);
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }

    *ge(key: IndexKey, t: _Transaction): Iterable<T> {
        const it = this.bin(t).ge(key);
        while (it.valid) {
            yield* it.value.values();
            it.next();
        }
    }

    *lt(key: IndexKey, t: _Transaction): Iterable<T> {
        const bin = this.bin(t);
        const limit = bin.lt(key);
        const it = bin.begin;
        if (!limit.valid) {
            // yield all
            while (it.valid) {
                yield* it.value.values();
                it.next();
            }
            return;
        }
        while (it.valid && limit.index >= it.index) {
            yield* it.value.values();
            it.next();
        }
        // const it = this.asBinary.lt(key);
        // while (it.valid) {
        //     yield* it.value.values();
        //     it.prev();
        // }
    }

    *le(key: IndexKey, t: _Transaction): Iterable<T> {
        const bin = this.bin(t);
        const limit = bin.le(key);
        const it = bin.begin;
        if (!limit.valid) {
            // yield all
            while (it.valid) {
                yield* it.value.values();
                it.next();
            }
            return;
        }
        while (it.valid && limit.index >= it.index) {
            yield* it.value.values();
            it.next();
        }
        // const it = this.asBinary.le(key);
        // while (it.valid) {
        //     yield* it.value.values();
        //     it.prev();
        // }
    }
}