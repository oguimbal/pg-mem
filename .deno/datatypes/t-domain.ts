import { DataType, nil, QueryError, _IType, _ISchema, _Transaction, _IRelation } from '../interfaces-private.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../evaluator.ts';
import hash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';

export interface DomainCheck {
    name: string;
    /** evaluate the CHECK for a candidate value (already cast to the base type) */
    run: (value: any, t: _Transaction | nil) => any;
}

export function asDomain(o: _IRelation | null): DomainType | null {
    return o && o.type === 'type' && o instanceof DomainType ? o : null;
}

/**
 * A DOMAIN is a base type plus optional NOT NULL / CHECK constraints. It behaves exactly
 * like its base type outward (same `primary`), and validates values built into it.
 */
export class DomainType extends TypeBase<any> {

    get primary(): DataType {
        // registered (and identified) under its own name, like an enum; transparency to
        // the base type is provided by the implicit cast below
        return this._name as any;
    }

    get name(): string {
        return this._name;
    }

    constructor(
        readonly schema: _ISchema,
        private readonly _name: string,
        readonly base: _IType,
        readonly notNull: boolean,
        readonly checks: DomainCheck[],
    ) {
        super(null);
    }

    install(): this {
        this.schema._registerType(this);
        return this;
    }

    drop(t: _Transaction): void {
        this.schema._unregisterType(this);
    }

    // ---- outward: usable wherever the base type is (delegated to the base) ----
    doCanCast(to: _IType) {
        return this.base.canCast(to);
    }
    doCast(value: Evaluator, to: _IType): Evaluator | nil {
        // the stored value is already a base value, so retype-as-base then cast onward
        const asBase = value.setType(this.base) as Evaluator;
        return to === this.base ? asBase : asBase.cast(to) as Evaluator;
    }
    doCanConvertImplicit(to: _IType): boolean {
        return !!this.base.canConvertImplicit(to);
    }

    // ---- inward: build from anything the base accepts, validating constraints ----
    doCanBuildFrom(from: _IType) {
        return from.canCast(this.base);
    }
    doBuildFrom(value: Evaluator, from: _IType): Evaluator | nil {
        const asBase = (from === this.base ? value : value.cast(this.base)) as Evaluator;
        if (!this.checks.length && !this.notNull) {
            return asBase.setType(this);
        }
        // forceNotConstant: validate at evaluation time (real row + transaction), never
        // fold at build (the CHECK expression itself needs a transaction to evaluate)
        return new Evaluator(
            this,
            asBase.id,
            hash({ domain: this._name, v: asBase.hash }),
            asBase,
            (raw, t) => {
                // invoke the base value the way the engine does internally (its `.get()`
                // guards against a nullish row, which we must not trip here)
                const v = typeof asBase.val === 'function'
                    ? (asBase.val as any)(raw, t)
                    : asBase.val;
                if (v === null || v === undefined) {
                    if (this.notNull) {
                        throw new QueryError(`domain ${this._name} does not allow null values`, '23502');
                    }
                    return null;
                }
                for (const c of this.checks) {
                    if (c.run(v, t) !== true) {
                        throw new QueryError(`value for domain ${this._name} violates check constraint "${c.name}"`, '23514');
                    }
                }
                return v;
            },
            { forceNotConstant: true },
        );
    }
}
