import { _ITable, _IView, _Transaction, _ISchema, QueryError } from '../interfaces-private.ts';
import { getTriggerRunner, TriggerContext } from './plpgsql.ts';

export type TriggerTiming = 'before' | 'after' | 'instead of';
export type TriggerOp = 'insert' | 'update' | 'delete';

export interface Trigger {
    name: string;
    timing: TriggerTiming;
    events: TriggerOp[];
    forEach: 'row' | 'statement';
    functionName: string;
    functionSchema: _ISchema;
    /** literal arguments from EXECUTE FUNCTION f(args) - exposed as TG_ARGV */
    arguments?: string[];
    /** compiled WHEN condition (NEW/OLD in scope); the trigger fires only if it is true */
    when?: (ctx: TriggerContext, t: _Transaction) => any;
    /** for `UPDATE OF a, b`: column ids to watch - fire only if one of them changed */
    updateColumns?: string[];
}

export interface TableTriggers {
    triggers: Trigger[];
}

export function emptyTriggers(): TableTriggers {
    return { triggers: [] };
}

/** Returned by fireRowTriggers when a BEFORE trigger vetoes the operation. */
export const SKIP_ROW = Symbol('skip-row');

/**
 * Fire the row-level triggers of a table for a given timing and operation.
 * For BEFORE triggers the (possibly modified) NEW row is threaded through and returned;
 * a trigger returning null aborts the operation (caller skips it). AFTER triggers run
 * for their side effects and the return value is ignored.
 * Returns the NEW row to use, or SKIP_ROW if a BEFORE trigger vetoed the operation.
 */
export function fireRowTriggers(
    table: _ITable,
    timing: 'before' | 'after',
    op: TriggerOp,
    newRow: any | null,
    oldRow: any | null,
    t: _Transaction,
): any | typeof SKIP_ROW {
    let current = newRow;
    for (const trig of table.triggers.triggers) {
        if (trig.timing !== timing || trig.forEach !== 'row' || !trig.events.includes(op)) {
            continue;
        }
        // `UPDATE OF cols`: fire only when one of the listed columns actually changed
        if (op === 'update' && trig.updateColumns && oldRow && current
            && !trig.updateColumns.some(c => distinct(oldRow[c], current[c]))) {
            continue;
        }
        const ctx: TriggerContext = {
            table,
            new: current,
            old: oldRow,
            op: op.toUpperCase() as TriggerContext['op'],
            ...triggerMeta(trig),
        };
        // WHEN condition (NEW/OLD in scope) gates the firing
        if (trig.when && trig.when(ctx, t) !== true) {
            continue;
        }
        const fn = trig.functionSchema.getFunction(trig.functionName, []);
        const runner = fn && getTriggerRunner(fn.implementation);
        if (!runner) {
            throw new QueryError(`trigger "${trig.name}" references function "${trig.functionName}" which is not a trigger function`);
        }
        const result = runner(ctx, t);
        if (timing === 'before') {
            // BEFORE ROW: returning NULL vetoes the operation; otherwise (for INSERT/
            // UPDATE) the returned row is threaded forward
            if (result === null || result === undefined) {
                return SKIP_ROW;
            }
            if (op !== 'delete') {
                current = result;
            }
        }
    }
    return current;
}

/** `IS DISTINCT FROM` semantics for the UPDATE OF change-detection (null-aware). */
function distinct(a: any, b: any): boolean {
    if (a === b) {
        return false;
    }
    if (a instanceof Date && b instanceof Date) {
        return +a !== +b;
    }
    if ((a === null || a === undefined) && (b === null || b === undefined)) {
        return false;
    }
    return true;
}

/**
 * Fire the statement-level triggers of a table for a given timing and operation. These run
 * once per statement (even when no rows are affected) and have no NEW/OLD row; the return
 * value is ignored.
 */
export function fireStatementTriggers(
    table: _ITable,
    timing: 'before' | 'after',
    op: TriggerOp,
    t: _Transaction,
): void {
    for (const trig of table.triggers.triggers) {
        if (trig.timing !== timing || trig.forEach !== 'statement' || !trig.events.includes(op)) {
            continue;
        }
        const fn = trig.functionSchema.getFunction(trig.functionName, []);
        const runner = fn && getTriggerRunner(fn.implementation);
        if (!runner) {
            throw new QueryError(`trigger "${trig.name}" references function "${trig.functionName}" which is not a trigger function`);
        }
        runner({ table, new: null, old: null, op: op.toUpperCase() as TriggerContext['op'], ...triggerMeta(trig) }, t);
    }
}

/**
 * Fire a view's INSTEAD OF row triggers for an operation. Returns true if a trigger handled
 * it (so the caller performs no real mutation), false if there was none.
 */
export function fireInsteadOf(
    view: _IView,
    op: TriggerOp,
    newRow: any | null,
    oldRow: any | null,
    t: _Transaction,
): boolean {
    let fired = false;
    for (const trig of view.triggers.triggers) {
        if (trig.timing !== 'instead of' || trig.forEach !== 'row' || !trig.events.includes(op)) {
            continue;
        }
        const fn = trig.functionSchema.getFunction(trig.functionName, []);
        const runner = fn && getTriggerRunner(fn.implementation);
        if (!runner) {
            throw new QueryError(`trigger "${trig.name}" references function "${trig.functionName}" which is not a trigger function`);
        }
        // the view's selection provides NEW/OLD column resolution for the runner
        runner({ table: view as any, new: newRow, old: oldRow, op: op.toUpperCase() as TriggerContext['op'], ...triggerMeta(trig) }, t);
        fired = true;
    }
    return fired;
}

/** does a view have an INSTEAD OF trigger for this operation? */
export function hasInsteadOf(view: _IView, op: TriggerOp): boolean {
    return view.triggers.triggers.some(tr => tr.timing === 'instead of' && tr.events.includes(op));
}

/** the TG_* metadata fields for a trigger */
function triggerMeta(trig: Trigger): Partial<TriggerContext> {
    return {
        name: trig.name,
        when: trig.timing.toUpperCase(),
        level: trig.forEach.toUpperCase(),
        args: trig.arguments ?? [],
    };
}
