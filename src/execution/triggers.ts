import { _ITable, _Transaction, _ISchema, QueryError } from '../interfaces-private';
import { getTriggerRunner, TriggerContext } from './plpgsql';

export type TriggerTiming = 'before' | 'after' | 'instead of';
export type TriggerOp = 'insert' | 'update' | 'delete';

export interface Trigger {
    name: string;
    timing: TriggerTiming;
    events: TriggerOp[];
    forEach: 'row' | 'statement';
    functionName: string;
    functionSchema: _ISchema;
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
        const fn = trig.functionSchema.getFunction(trig.functionName, []);
        const runner = fn && getTriggerRunner(fn.implementation);
        if (!runner) {
            throw new QueryError(`trigger "${trig.name}" references function "${trig.functionName}" which is not a trigger function`);
        }
        const ctx: TriggerContext = {
            table,
            new: current,
            old: oldRow,
            op: op.toUpperCase() as TriggerContext['op'],
        };
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
