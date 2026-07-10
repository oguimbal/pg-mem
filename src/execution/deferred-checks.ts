import { _Transaction } from '../interfaces-private';

// Constraint checks postponed to the end of the transaction (DEFERRABLE INITIALLY
// DEFERRED). They accumulate on the transaction and run just before it is finalized to
// root, so a violation aborts the whole batch and nothing persists.

const DEFERRED_CHECKS = Symbol('deferred_checks');

export function deferCheck(t: _Transaction, check: (t: _Transaction) => void): void {
    const cur = t.get<((t: _Transaction) => void)[]>(DEFERRED_CHECKS) ?? [];
    t.set(DEFERRED_CHECKS, [...cur, check]);
}

export function runDeferredChecks(t: _Transaction): void {
    const checks = t.get<((t: _Transaction) => void)[]>(DEFERRED_CHECKS);
    if (!checks?.length) {
        return;
    }
    // clear before running so re-entrancy can't loop
    t.set(DEFERRED_CHECKS, []);
    for (const check of checks) {
        check(t);
    }
}
