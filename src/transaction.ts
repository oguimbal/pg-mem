import { Map as ImMap, Set as ImSet } from 'immutable';
import { NotSupported } from './interfaces';
import { _Transaction } from './interfaces-private';

export class Transaction implements _Transaction {
    private origData: ImMap<symbol, any>;
    private transientData: any = {};

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

    serialize(): string {
        return JSON.stringify({ data: this.data.toJS() });
    }

    static deserialize(serialized: string): Transaction {
        const parsed = JSON.parse(serialized);
        const data = ImMap<symbol, any>(parsed.data);
        return new Transaction(null, data);
    }
}
