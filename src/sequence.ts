import { AlterSequenceChange, AlterSequenceSetOptions, CreateSequenceOptions } from 'pgsql-ast-parser';
import { ignore } from './utils';
import { NotSupported, asTable, _ISchema, _ISequence, _IType, _Transaction } from './interfaces-private';
import { DataType, ISubscription, QueryError } from './interfaces';
import { fromNative, makeType, Types } from './datatypes';

export class Sequence implements _ISequence {

    get type(): 'sequence' {
        return 'sequence';
    }

    private symbol = Symbol();
    private owner?: ISubscription;
    private cfg: {
        start?: number;
        max?: number;
        min?: number;
        cycle?: boolean;
        inc?: number;
        dataType?: _IType;
    } = {};



    get cycle() {
        return this.cfg.cycle ?? false;
    }

    get dataType() {
        return this.cfg.dataType ?? Types.int;
    }

    get inc() {
        return this.cfg.inc ?? 1;
    }


    constructor(public name: string, private db: _ISchema) {

    }

    get start() {
        return this.cfg.start ?? (this.inc > 0
            ? this.min
            : this.max);
    }

    get max() {
        return this.cfg.max
            ?? (this.inc > 0
                ? Number.MAX_SAFE_INTEGER - 1
                : -1);
    }

    get min() {
        return this.cfg.min
            ?? (this.inc > 0
                ? 1
                : Number.MIN_SAFE_INTEGER + 1);
    }

    alter(t: _Transaction, opts: CreateSequenceOptions | AlterSequenceChange): this {
        const oldCfg = { ...this.cfg };
        try {
            if (!('type' in opts)) {
                return this.alterOpts(t, opts);
            }
            switch (opts.type) {
                case 'set options':
                    this.alterOpts(t, opts);
                    if (opts.restart) {
                        t.set(this.symbol, this.start);
                    }
                    return this;
                case 'set schema':
                    if (opts.newSchema === this.db.name) {
                        return this;
                    }
                    throw new NotSupported('Sequence schema change');
                case 'rename':
                    const to = opts.newName.toLowerCase();
                    this.db._doRenSeq(this.name, to);
                    this.name = to;
                    return this;
                case 'owner to':
                    // todo: implement sequence owners ? ...ignored to support pg_dump exports.
                    ignore(opts);
                    return this;
                default:
                    throw NotSupported.never(opts);
            }
        } catch (e) {
            this.cfg = oldCfg;
            throw e;
        }
    }

    nextValue(t: _Transaction): number {
        let ret = t.get<number>(this.symbol) ?? this.start;
        const value = ret + this.inc;
        if (value > this.max) {
            throw new QueryError(`Sequence ${this.name} reached its maximum value`);
        }
        if (value < this.min) {
            throw new QueryError(`Sequence ${this.name} reached its minimum value`);
        }
        t.set(this.symbol, value);
        return ret;
    }

    private alterOpts(t: _Transaction, opts: CreateSequenceOptions) {
        if (opts.as) {
            ignore(opts.as);
            this.cfg.dataType = fromNative(opts.as);
        }
        ignore(opts.cache);
        if (opts.cycle) {
            this.cfg.cycle = opts.cycle === 'cycle';
        }

        if (typeof opts.incrementBy === 'number') {
            this.cfg.inc = opts.incrementBy;
        }

        if (typeof opts.maxValue === 'number') {
            this.cfg.max = opts.maxValue;
        } else if (opts.maxValue) {
            this.cfg.max = undefined;
        }

        if (typeof opts.minValue === 'number') {
            this.cfg.min = opts.minValue;
        } else if (opts.maxValue) {
            this.cfg.min = undefined;
        }

        if (typeof opts.startWith === 'number') {
            this.cfg.start = opts.startWith;
        }


        if (opts.ownedBy === 'none') {
            this.owner?.unsubscribe();
        } else if (opts.ownedBy) {
            this.owner?.unsubscribe();

            const owner = asTable(this.db.getObject({
                name: opts.ownedBy.table,
                schema: opts.ownedBy.schema
            })).getColumnRef(opts.ownedBy.column);

            this.owner = owner.onDrop(dt => this.drop(dt));
        }

        // === validate
        if (this.max < this.min) {
            throw new QueryError('Invalid squeuence min-max');
        }

        if (!this.inc) {
            throw new QueryError('Invalid increment');
        }

        if (this.start > this.max || this.start < this.min) {
            throw new QueryError('Invalid sequence starting value');
        }
        return this;
    }

    drop(t: _Transaction) {
        this.owner?.unsubscribe();
        this.owner = undefined;
        this.db._dropSeq(this.name);
        t.delete(this.symbol);
    }
}