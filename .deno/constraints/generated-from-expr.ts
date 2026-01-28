import { ISubscription, NotSupported, QueryError } from '../interfaces.ts';
import { AlterColumnAddGenerated, Expr, nil } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { _Column, _IConstraint, _ITable, _Transaction } from '../interfaces-private.ts';
import { deepEqual, nullIsh } from '../utils.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { withSelection } from '../parser/context.ts';
import { MemoryTable } from '../table.ts';

export class GeneratedComputedConstraint implements _IConstraint {
    private subs: ISubscription[] = [];

    private get table() {
        return this.column.table as MemoryTable;
    }
    private get schema() {
        return this.table.ownerSchema;
    }

    constructor(readonly name: string | nil, private column: _Column, private expression: Expr) {
    }

    uninstall(t: _Transaction): void {
        for (const s of this.subs) {
            s.unsubscribe();
        };
        this.subs = [];
    }

    install(ct: _Transaction, _c: AlterColumnAddGenerated) {
        const evaluator = withSelection(this.table.selection, () => buildValue(this.expression));
        this.subs.push(this.table.onBeforeChange('all', (old, neu, dt, opts) => {
            if (!deepEqual(old, neu)) {
                throw new QueryError(`Column "${this.column.name}" is a generated column.`, '42601');
            }
            const newValue = evaluator.get(neu, dt);
            neu[this.column.name] = newValue;
        }));

        // compute the values for the existing rows
        this.table.remapData(ct, row => {
            const newValue = evaluator.get(row, ct);
            row[this.column.name] = newValue;
        });
    }

}