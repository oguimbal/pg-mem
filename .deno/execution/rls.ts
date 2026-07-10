import { Expr, PolicyCommand } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';

// Row-level security state stored per table. Policy predicates are kept as parsed AST
// (compiled lazily against the table selection during enforcement, see slice C).

export interface Policy {
    name: string;
    /** true = PERMISSIVE (OR-combined), false = RESTRICTIVE (AND-combined) */
    permissive: boolean;
    command: PolicyCommand;
    /** role names the policy applies to; empty means PUBLIC (everyone) */
    roles: string[];
    using?: Expr | null;
    withCheck?: Expr | null;
}

export interface TableRls {
    enabled: boolean;
    /** FORCE makes RLS apply to the table owner too */
    forced: boolean;
    policies: Policy[];
}

export function emptyRls(): TableRls {
    return { enabled: false, forced: false, policies: [] };
}

/** Does a policy apply to the given role name? (PUBLIC or an explicit match) */
export function policyAppliesToRole(policy: Policy, roleName: string): boolean {
    if (!policy.roles.length || policy.roles.includes('public')) {
        return true;
    }
    return policy.roles.includes(roleName);
}

/** Does a policy apply to the given command? */
export function policyAppliesToCommand(policy: Policy, command: 'select' | 'insert' | 'update' | 'delete'): boolean {
    return policy.command === 'all' || policy.command === command;
}
