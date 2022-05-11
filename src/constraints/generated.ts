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
    constructor(readonly name: string | nil, private column: _Column) {}

    uninstall(t: _Transaction): void {
        this.sub?.unsubscribe();
    }

    install(ct: _Transaction, _c: AlterColumnAddGenerated) {
        if (!this.column.notNull) {
            // if it's a table creation, then force 'not null'
            const tableCreation = !this.schema.getTable(this.table.name, true);
            if (tableCreation) {
                this.column.alter(
                    {
                        type: 'set not null',
                    },
                    ct,
                );
            } else {
                // else, throw an error
                throw new QueryError(
                    `column "${this.column.name}" of relation "${this.table.name}" must be declared NOT NULL before identity can be added`,
                );
            }
        }

        const seq = this.schema.createSequence(ct, _c.sequence, _c.sequence?.name);

        // todo : Review this... it's a complete bluff (dont have time to check spec)
        const mode = _c.always ?? 'always';
        this.sub = this.table.onBeforeChange([this.column], (old, neu, dt, opts) => {
            // only act on new things
            if (old) {
                return;
            }
            const gen = () => (neu[this.column.name] = seq.nextValue(dt));

            if (nullIsh(neu[this.column.name])) {
                // no value has been provided => generate one.
                gen();
                return;
            }

            // a value has been provided => check if must be overriden.
            switch (mode) {
                case 'by default':
                    switch (opts.overriding ?? 'system') {
                        case 'system':
                            break;
                        default:
                            gen();
                            break;
                    }
                    break;
                case 'always':
                    // column is 'GENREATED ALWAYS'
                    // => must specify 'overriding system value'
                    if (opts.overriding !== 'system') {
                        throw new QueryError({
                            error: `cannot insert into column "${this.column.name}"`,
                            details: ` Column "${this.column.name}" is an identity column defined as GENERATED ALWAYS.`,
                            hint: 'Use OVERRIDING SYSTEM VALUE to override.',
                        });
                    }
                    break;
                default:
                    throw NotSupported.never(mode);
            }
        });
    }
}
