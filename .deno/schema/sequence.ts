import { AlterSequenceChange, CreateSequenceOptions } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { combineSubs, ignore, nullIsh } from '../utils.ts';
import { NotSupported, asTable, _ISchema, _ISequence, _IType, _Transaction, RegClass, Reg } from '../interfaces-private.ts';
import { ISubscription, nil, QueryError } from '../interfaces.ts';
import { Types } from '../datatypes/index.ts';

interface SeqData {
    currval: number | undefined;
    nextval: number;
}

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

    readonly reg: Reg;


    get cycle() {
        return this.cfg.cycle ?? false;
    }

    get dataType() {
        return this.cfg.dataType ?? Types.integer;
    }

    get inc() {
        return this.cfg.inc ?? 1;
    }


    constructor(public name: string, readonly ownerSchema: _ISchema) {
        this.reg = ownerSchema._reg_register(this);
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

    alter(t: _Transaction, opts: CreateSequenceOptions | AlterSequenceChange | nil): this {
        if (!opts) {
            return this;
        }
        const oldCfg = { ...this.cfg };
        try {
            if (!('type' in opts)) {
                return this.alterOpts(t, opts);
            }
            switch (opts.type) {
                case 'set options':
                    this.alterOpts(t, opts);
                    if (opts.restart === true || typeof opts.restart === 'number') {
                        if (typeof opts.restart === 'number') {
                            if (opts.restart < this.min) {
                                throw new QueryError(`RESTART value (${opts.restart}) cannot be less than MINVALUE (${this.min})`, '22023');
                            }
                            this.cfg.start = opts.restart;
                        }
                        const data: SeqData = {
                            currval: t.get<SeqData>(this.symbol)?.currval,
                            nextval: this.start,
                        }
                        t.set(this.symbol, data);
                    }
                    return this;
                case 'set schema':
                    if (opts.newSchema.name === this.ownerSchema.name) {
                        return this;
                    }
                    throw new NotSupported('Sequence schema change');
                case 'rename':
                    const to = opts.newName.name.toLowerCase();
                    this.ownerSchema._reg_rename(this, this.name, to);
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
        let v = t.get<SeqData>(this.symbol)?.nextval;
        if (v === undefined) {
            v = this.start;
        }
        this.setValue(t, v);
        return v;
    }

    setValue(t: _Transaction, value: number) {
        if (value > this.max) {
            throw new QueryError(`reached maximum value of sequence "${this.name}"`);
        }
        if (value < this.min) {
            throw new QueryError(`reached minimum value of sequence "${this.name}"`);
        }
        const data: SeqData = {
            currval: value,
            nextval: value + this.inc,
        };
        t.set(this.symbol, data);
    }

    restart(t: _Transaction) {
        t.delete(this.symbol);
    }

    currentValue(t: _Transaction): number {
        const v = t.get<SeqData>(this.symbol)?.currval;
        if (v === undefined) {
            throw new QueryError(`currval of sequence "${this.name}" is not yet defined in this session`, '55000');
        }
        return v;
    }


    private alterOpts(t: _Transaction, opts: CreateSequenceOptions) {
        if (opts.as) {
            ignore(opts.as);
            this.cfg.dataType = this.ownerSchema.getType(opts.as);
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

            const tbl = asTable(this.ownerSchema.getObject({
                name: opts.ownedBy.table,
                schema: opts.ownedBy.schema
            }));

            const owner = tbl.getColumnRef(opts.ownedBy.column);

            this.owner = combineSubs(
                owner.onDrop(dt => this.drop(dt)),
                tbl.onDrop(dt => this.drop(dt)),
            );
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
        t.delete(this.symbol);
        this.ownerSchema._reg_unregister(this);
    }
}