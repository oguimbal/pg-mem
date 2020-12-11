import { ISubscription, NotSupported, QueryError } from '../interfaces';
import { AlterColumnAddGenerated, Expr, ExprBinary, nil, TableConstraintForeignKey } from 'pgsql-ast-parser';
import { asTable, CreateIndexColDef, _Column, _IConstraint, _ITable, _Transaction } from '../interfaces-private';
import { nullIsh } from '../utils';

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
            throw new QueryError(`column "${this.column.name}" of relation "${this.table.name}" must be declared NOT NULL before identity can be added`);
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