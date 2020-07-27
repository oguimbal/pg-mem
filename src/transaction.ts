import { _Transaction } from './interfaces-private';
import { Map as ImMap } from 'immutable';
import { NotSupported, QueryError } from './interfaces';

export class Transaction implements _Transaction {
    private origData: ImMap<symbol, any>;

    static root() {
        return new Transaction(null, ImMap());
    }

    get isChild() {
        return !!this.parent;
    }

    private constructor(private parent: Transaction, private data: ImMap<symbol, any>) {
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

    fullCommit() {
        const ret = this.commit();
        return ret.isChild
            ? ret.fullCommit()
            : ret;
    }

    rollback () {
        return this.parent ?? this;
    }
}
