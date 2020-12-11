import { ISubscription, NotSupported, QueryError } from '../interfaces.ts';
import { AlterColumnAddGenerated, Expr, ExprBinary, nil, TableConstraintForeignKey } from 'https://deno.land/x/pgsql_ast_parser@1.4.2/mod.ts';
import { asTable, CreateIndexColDef, _Column, _IConstraint, _ITable, _Transaction } from '../interfaces-private.ts';
import { nullIsh } from '../utils.ts';

export class GeneratedIdentityConstraint implements _IConstraint {
    private sub?: ISubscription;

    private get table() {
        return this.column.table;
    }
    private get schema() {
        return this.table.ownerSchema;
    }
    constructor(readonly name: string | nil, private column: _Column) {
    }

    uninstall(t: _Transaction): void {
        this.sub?.unsubscribe();
    }


    install(ct: _Transaction, _c: AlterColumnAddGenerated) {
        if (!this.column.notNull) {
            // if it's a table creation, then force 'not null'
            const tableCreation = !this.schema.getTable(this.table.name, true);
            if (tableCreation) {
                this.column.alter({
                    type: 'set not null',
                }, ct);
            } else {
                // else, throw an error
                throw new QueryError(`column "${this.column.name}" of relation "${this.table.name}" must be declared NOT NULL before identity can be added`);
            }
        }

        const seq = this.schema.createSequence(ct, _c.sequence, _c.sequence?.name);

        // todo : Review this... it's a complete bluff (dont have time to check spec)
        const mode = _c.always;
        this.sub = this.table.onBeforeChange([this.column], (old, neu, dt) => {
            // only act on new things
            if (old) {
                return;
            }

            if (mode === 'always' || nullIsh(neu[this.column.name])) {
                neu[this.column.name] = seq.nextValue(dt);
            }
        });
    }

}