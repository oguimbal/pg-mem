import { IValue, _IIndex, _ITable, getId, IndexKey, CreateIndexColDef, _Transaction, _Explainer, _IndexExplanation, IndexExpression, IndexOp, Stats, _INamedIndex, Reg, _ISchema } from '../interfaces-private.ts';
// @ts-ignore
import createTree from 'https://deno.land/x/functional_red_black_tree@1.0.1-deno/mod.ts';
import { QueryError, NotSupported, nil } from '../interfaces.ts';
import { Set as ImSet, Map as ImMap } from 'https://deno.land/x/immutable@4.0.0-rc.12-deno.1/mod.ts';
import { deepCloneSimple, nullIsh, hasNullish } from '../utils.ts';


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
    forEach(fn: (key: IndexKey, value: T) => boolean, low?: number, high?: number): void;
    // root;
}

interface BIterator<T> {
    readonly key: IndexKey;
    readonly value: T;
    // tree;
    readonly index: any;
    readonly valid: boolean;
    clone(): BIterator<T>;
    remove(): BTree<T>;
    update(value: T): BTree<T>;
    next(): void;
    prev(): void;
    readonly hasNext: boolean;
    readonly hasPrev: boolean;
}

type RawTree<T> = BTree<ImMap<string, T>>;

export class BIndex<T = any> implements _INamedIndex<T> {

    get type(): 'index' {
        return 'index';
    }

    readonly reg: Reg;

    // private asBinary: RawTree;
    expressions: (IndexExpression & IValue)[];
    private treeBinId = Symbol();
    private treeCountId = Symbol();


    get ownerSchema(): _ISchema {
        return this.onTable.ownerSchema;
    }

    constructor(t: _Transaction
        , readonly name: string
        , readonly cols: readonly CreateIndexColDef[]
        , readonly onTable: _ITable<T>
        , readonly hash: string
        , readonly unique: boolean
        , readonly notNull: boolean
        , readonly predicate: IValue | nil) {
        this.reg = onTable.ownerSchema._reg_register(this);
        this.truncate(t);
        this.expressions = cols.map(x => x.value);
    }

    drop(t: _Transaction): void {
        this.onTable.dropIndex(t, this.name);
    }

    compare(_a: any, _b: any) {
        for (let i = 0; i < this.expressions.length; i++) {
            const k = this.cols[i];
            const a = _a[i];
            const b = _b[i];
            const an = nullIsh(a);
            const bn = nullIsh(b);
            if (an || bn) {
                if (an === bn) {
                    continue;
                }
                return (an
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

    buildKey(raw: any, t: _Transaction): any[] {
        return this.expressions.map(k => k.get(raw, t));
    }

    truncate(t: _Transaction) {
        const asBinary = createTree((a: any, b: any) => {
            return this.compare(a, b);
        });
        this.setBin(t, asBinary);
    }

    dropFromData(t: _Transaction) {
        t.delete(this.treeBinId);
    }

    private bin(t: _Transaction) {
        return t.get<RawTree<T>>(this.treeBinId);
    }
    private setBin(t: _Transaction, val: RawTree<T>) {
        return t.set(this.treeBinId, val);
    }

    private setCount(t: _Transaction, val: number) {
        return t.set(this.treeCountId, val);
    }
    private getCount(t: _Transaction): number {
        return t.get<number>(this.treeCountId) ?? 0;
    }

    hasKey(key: IndexKey[], t: _Transaction): boolean {
        const it = this.bin(t).find(key);
        return it.valid;
    }

    add(raw: T, t: _Transaction) {
        // check that predicate is OK
        if (this.predicate) {
            const val = this.predicate.get(raw, t);
            if (nullIsh(val) || val === false) {
                return;
            }
        }

        // build key and object id
        const id = getId(raw);
        const key = this.buildKey(raw, t);
        const hasNil = hasNullish(...key);
        if (this.notNull && hasNil) {
            throw new QueryError('Cannot add a null record in index ' + this.name);
        }
        if (this.unique && !hasNil && this.hasKey(key, t)) {
            const idCols = this.cols.map(it => it.value.id);
            throw new QueryError({
                error: `insert into "${this.onTable.name}" (${Object.keys(raw as any).join(', ')}) `
                    + `values (${Object.keys(raw as any).map((_, i) => `$${i + 1}`).join(', ')}) returning "${idCols}" `
                    + `- duplicate key value violates unique constraint "${this.onTable.name}_pkey"`,
                details: `Key (${idCols})=(${key}) already exists.`,
                code: '23505'
            });
        }
        // get tree
        let tree = this.bin(t);
        // get key in tree
        let keyValues = tree.find(key);
        if (keyValues.valid) {
            if (keyValues.value.has(id)) {
                return; // already exists
            }
            tree = keyValues.update(keyValues.value.set(id, raw));
        } else {
            tree = tree.insert(key, ImMap<string, T>().set(id, raw));
        }
        this.setBin(t, tree);
        this.setCount(t, this.getCount(t) + 1);

    }

    delete(raw: any, t: _Transaction) {
        const key = this.buildKey(raw, t);
        let tree = this.bin(t);
        let keyValues = tree.find(key);
        if (!keyValues.valid) {
            return; // key does not exists
        }
        const id = getId(raw);
        if (!keyValues.value.has(id)) {
            return; // element does not exists
        }
        const newKeyValues = keyValues.value.delete(id);
        if (!newKeyValues.size) {
            tree = keyValues.remove();
        } else {
            tree = keyValues.update(newKeyValues);
        }
        this.setBin(t, tree);
        this.setCount(t, this.getCount(t) - 1);
    }

    eqFirst(rawKey: IndexKey, t: _Transaction): T | null {
        for (const r of this.eq(rawKey, t, false)) {
            return deepCloneSimple(r);
        }
        return null;
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


    entropy(op: IndexOp) {
        const bin = this.bin(op.t);
        if (!bin.length) {
            return 0;
        }
        const all = op.t.get<number>(this.treeCountId) ?? 0;
        // evaluate number of keys included in this operation
        const e = this._keyCount(op);
        // multiply by average values per key
        return e * all / bin.length;
    }

    stats(t: _Transaction, key?: IndexKey): Stats {
        if (!key) {
            return {
                count: t.get<number>(this.treeCountId) ?? 0,
            };
        }
        const found = this.bin(t).get(key);
        return {
            count: found?.size ?? 0,
        };
    }

    iterateKeys(t: _Transaction): Iterable<IndexKey> {
        const bin = this.bin(t);
        return bin.keys;
    }


    private _keyCount(op: IndexOp) {
        const bin = this.bin(op.t);
        switch (op.type) {
            case 'eq': {
                const begin = bin.find(op.key);
                if (!begin.valid) {
                    return 0;
                }
                const end = bin.gt(op.key);
                if (!end.valid) {
                    return bin.length - begin.index;
                }
                return end.index - begin.index + 1;
            }
            case 'neq': {
                let cnt = 0;
                const first = bin.find(op.key);
                if (!first.valid) {
                    return bin.length;
                }
                cnt += first.valid
                    ? first.index
                    : 0;
                const end = bin.gt(op.key);
                cnt += end.valid
                    ? (bin.length - end.index)
                    : 0;
                return cnt;
            }
            case 'ge': {
                const found = bin.ge(op.key);
                return found.valid
                    ? (bin.length - found.index)
                    : 0;
            }
            case 'gt': {
                const found = bin.gt(op.key);
                return found.valid
                    ? (bin.length - found.index)
                    : 0;
            }
            case 'le': {
                const found = bin.gt(op.key);
                return found.valid
                    ? found.index
                    : bin.length;
            }
            case 'lt': {
                const found = bin.ge(op.key);
                return found.valid
                    ? found.index
                    : bin.length;
            }
            case 'inside': {
                const begin = bin.ge(op.lo);
                if (!begin.valid) {
                    return 0;
                }
                const end = bin.gt(op.hi);
                if (!end.valid) {
                    return bin.length - begin.index;
                }
                return end.index - begin.index;
            }
            case 'outside': {
                let cnt = 0;
                const first = bin.lt(op.lo);
                cnt += first.valid
                    ? first.index + 1
                    : 0;
                const end = bin.gt(op.hi);
                cnt += end.valid
                    ? (bin.length - end.index)
                    : 0;
                return cnt;
            }
            case 'nin': {
                let cnt = bin.length;
                for (const e of op.keys) {
                    const f = bin.find(e);
                    if (f.valid) {
                        cnt--;
                    }
                }
                return cnt;
            }
            default:
                throw NotSupported.never(op['type']);
        }
    }

    *enumerate(op: IndexOp): Iterable<T> {
        for (const x of this._enumerate(op)) {
            yield deepCloneSimple(x);
        }
    }
    private _enumerate(op: IndexOp): Iterable<T> {
        switch (op.type) {
            case 'eq':
                return this.eq(op.key, op.t, op.matchNull!);
            case 'neq':
                return this.neq(op.key, op.t, op.matchNull!);
            case 'ge':
                return this.ge(op.key, op.t);
            case 'le':
                return this.le(op.key, op.t);
            case 'gt':
                return this.gt(op.key, op.t);
            case 'lt':
                return this.lt(op.key, op.t);
            case 'outside':
                return this.outside(op.lo, op.hi, op.t);
            case 'inside':
                return this.inside(op.lo, op.hi, op.t);
            case 'nin':
                return this.nin(op.keys, op.t);
            default:
                throw NotSupported.never(op['type']);
        }
    }

    *eq(key: IndexKey, t: _Transaction, matchNull: boolean): Iterable<T> {
        if (!matchNull && key.some(nullIsh)) {
            return;
        }
        const it = this.bin(t).find(key);
        while (it.valid && this.compare(it.key, key) === 0) {
            yield* it.value.values();
            it.next();
        }
    }



    *neq(key: IndexKey, t: _Transaction, matchNull: boolean): Iterable<T> {
        if (!matchNull && key.some(nullIsh)) {
            return;
        }
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

    *outside(lo: IndexKey, hi: IndexKey, t: _Transaction): Iterable<T> {
        yield* this.lt(lo, t);
        yield* this.gt(hi, t);
    }

    *inside(lo: IndexKey, hi: IndexKey, t: _Transaction): Iterable<T> {
        const it = this.bin(t).ge(lo);
        while (it.valid && this.compare(it.key, hi) <= 0) {
            yield* it.value.values();
            it.next();
        }
    }

    explain(e: _Explainer): _IndexExplanation {
        return {
            _: 'btree',
            onTable: this.onTable.name,
            btree: this.expressions.map(x => x.id!),
        }
    }
}