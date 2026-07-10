import { _ISelection, _ITable, _Transaction, IValue, Row, _Explainer, _SelectExplanation, Stats, QueryError } from '../interfaces-private';
import { FilterBase } from '../transforms/transform-base';
import { Types } from '../datatypes';
import { buildValue } from '../parser/expression-builder';
import { withSelection } from '../parser/context';
import { currentRole } from './roles';
import { Policy, policyAppliesToCommand, policyAppliesToRole } from './rls';

export type RlsCommand = 'select' | 'insert' | 'update' | 'delete';

/** True when the current role skips RLS entirely (superuser or BYPASSRLS). */
export function bypassesRls(t: _Transaction): boolean {
    const role = currentRole(t);
    return role.superuser || role.bypassRls;
}

interface CompiledPolicy {
    policy: Policy;
    using: IValue | null;
    withCheck: IValue | null;
}

/** Compiles a policy's predicates against the table selection (once, at build time). */
function compilePolicies(selection: _ISelection, policies: Policy[]): CompiledPolicy[] {
    return withSelection(selection, () => policies.map(policy => ({
        policy,
        using: policy.using ? buildValue(policy.using).cast(Types.bool) : null,
        withCheck: policy.withCheck ? buildValue(policy.withCheck).cast(Types.bool) : null,
    })));
}

/**
 * Evaluates whether a row passes the RLS predicates applicable to the current role
 * and command. Permissive policies are OR-combined, restrictive AND-combined:
 *   (perm1 OR perm2 OR ...) AND restr1 AND restr2 ...
 * Returns false (deny) when no permissive policy applies.
 */
function rowPasses(compiled: CompiledPolicy[], kind: 'using' | 'withCheck', roleName: string, command: RlsCommand, row: Row, t: _Transaction): boolean {
    let sawPermissive = false;
    let permissiveOk = false;
    let restrictiveOk = true;
    for (const c of compiled) {
        if (!policyAppliesToRole(c.policy, roleName) || !policyAppliesToCommand(c.policy, command)) {
            continue;
        }
        // WITH CHECK falls back to USING when not specified (postgres behaviour)
        const pred = kind === 'withCheck' ? (c.withCheck ?? c.using) : c.using;
        const val = pred ? !!pred.get(row, t) : true; // no predicate = always true
        if (c.policy.permissive) {
            sawPermissive = true;
            permissiveOk = permissiveOk || val;
        } else {
            restrictiveOk = restrictiveOk && val;
        }
    }
    return sawPermissive && permissiveOk && restrictiveOk;
}

/** Runtime read-visibility filter: applied only when RLS is on and the role doesn't bypass. */
class RlsSelection extends FilterBase {
    private compiled: CompiledPolicy[];

    get index() {
        return null;
    }

    constructor(private sel: _ISelection, private table: _ITable, private command: RlsCommand) {
        super(sel);
        this.compiled = compilePolicies(sel, table.rls.policies);
    }

    entropy(t: _Transaction) {
        return this.sel.entropy(t);
    }

    stats(): Stats | null {
        return null;
    }

    hasItem(raw: Row, t: _Transaction): boolean {
        if (!this.enforced(t)) {
            return this.sel.hasItem(raw, t);
        }
        return this.sel.hasItem(raw, t)
            && rowPasses(this.compiled, 'using', currentRole(t).name, this.command, raw, t);
    }

    private enforced(t: _Transaction): boolean {
        return this.table.rls.enabled && !bypassesRls(t);
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        if (!this.enforced(t)) {
            yield* this.sel.enumerate(t);
            return;
        }
        const roleName = currentRole(t).name;
        for (const raw of this.sel.enumerate(t)) {
            if (rowPasses(this.compiled, 'using', roleName, this.command, raw, t)) {
                yield raw;
            }
        }
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            id: e.idFor(this),
            _: 'seqFilter',
            filtered: this.sel.explain(e),
        };
    }
}

/** Wrap a table's selection with row-level security read enforcement. */
export function applyReadRls(table: _ITable, selection: _ISelection, command: RlsCommand): _ISelection {
    if (!table.rls.policies.length && !table.rls.enabled) {
        return selection;
    }
    return new RlsSelection(selection, table, command);
}

/** Throws if a written row violates the WITH CHECK predicates for a command. */
export function checkWriteRls(table: _ITable, command: 'insert' | 'update', row: Row, t: _Transaction): void {
    if (!table.rls.enabled || bypassesRls(t)) {
        return;
    }
    const compiled = compilePolicies(table.selection, table.rls.policies);
    if (!rowPasses(compiled, 'withCheck', currentRole(t).name, command, row, t)) {
        throw new QueryError(`new row violates row-level security policy for table "${table.name}"`, '42501');
    }
}
