import { _Transaction } from './interfaces-private.ts';
import { Map as ImMap, Set as ImSet } from 'https://deno.land/x/immutable@4.0.0-rc.12-deno.1/mod.ts';
import { NotSupported, QueryError } from './interfaces.ts';

export class Transaction implements _Transaction {
    private origData: ImMap<symbol, any>;
    private transientData: any = {};
    /** named savepoints → the data snapshot captured when they were declared.
     * insertion order matters: rolling back to / releasing one discards later ones */
    private savepoints = new Map<string, ImMap<symbol, any>>();

    static root() {
        return new Transaction(null, ImMap());
    }

    get isChild() {
        return !!this.parent;
    }

    private constructor(private parent: Transaction | null, private data: ImMap<symbol, any>) {
        this.origData = data;
    }


    clone() {
        return new Transaction(null, this.data);
    }

    fork(): _Transaction {
        return new Transaction(this, this.data);
    }

    commit(): _Transaction {
        if (!this.parent) {
            return this;
        }
        if (this.parent.data !== this.origData) {
            throw new NotSupported('Concurrent transactions');
        }
        this.parent.data = this.data;
        return this.parent;
    }

    fullCommit() {
        const ret = this.commit();
        return ret.isChild
            ? ret.fullCommit()
            : ret;
    }

    rollback() {
        return this.parent ?? this;
    }

    savepoint(name: string): void {
        // re-declaring a name captures the current state under it (postgres hides the
        // older savepoint of the same name; we simply overwrite - close enough for v1)
        this.savepoints.set(name, this.data);
    }

    rollbackTo(name: string): void {
        const saved = this.savepoints.get(name);
        if (saved === undefined) {
            throw new QueryError(`savepoint "${name}" does not exist`);
        }
        this.data = saved;
        // the savepoint survives (can be rolled back to again), but any savepoints
        // established after it are discarded
        this.discardSavepointsAfter(name, false);
    }

    release(name: string): void {
        if (!this.savepoints.has(name)) {
            throw new QueryError(`savepoint "${name}" does not exist`);
        }
        this.discardSavepointsAfter(name, true);
    }

    private discardSavepointsAfter(name: string, inclusive: boolean): void {
        let reached = false;
        for (const k of [...this.savepoints.keys()]) {
            if (k === name) {
                reached = true;
                if (inclusive) {
                    this.savepoints.delete(k);
                }
                continue;
            }
            if (reached) {
                this.savepoints.delete(k);
            }
        }
    }

    delete(identity: symbol): void {
        this.data = this.data.delete(identity);
    }

    set<T>(identity: symbol, data: T): T {
        this.data = this.data.set(identity, data);
        return data;
    }

    get<T>(identity: symbol): T {
        return this.data.get(identity);
    }

    getMap<T extends ImMap<any, any>>(identity: symbol): T {
        let got = this.data.get(identity);
        if (!got) {
            this.data = this.data.set(identity, got = ImMap());
        }
        return got as any as T;
    }

    getSet<T>(identity: symbol): ImSet<T> {
        let got = this.data.get(identity);
        if (!got) {
            this.data = this.data.set(identity, got = ImSet());
        }
        return got as any;
    }

    setTransient<T>(identity: symbol, data: T): T {
        this.transientData[identity] = data as any;
        return data;
    }

    /** Set transient data, which will only exist within the scope of the current statement */
    getTransient<T>(identity: symbol): T {
        return this.transientData[identity] as T;
    }

    clearTransientData(): void {
        this.transientData = {};
    }
}
