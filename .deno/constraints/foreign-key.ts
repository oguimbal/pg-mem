import { ISubscription, NotSupported, QueryError } from '../interfaces.ts';
import { Expr, ExprBinary, TableConstraintForeignKey } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { asTable, CreateIndexColDef, _IConstraint, _ITable, _Transaction } from '../interfaces-private.ts';
import { nullIsh } from '../utils.ts';

export class ForeignKey implements _IConstraint {

    private unsubs: ISubscription[] = [];

    private table!: _ITable;
    private foreignTable!: _ITable;



    get db() {
        return this.table.ownerSchema.db;
    }

    get schema() {
        return this.table.ownerSchema;
    }


    constructor(readonly name: string) {
    }

    install(_t: _Transaction, cst: TableConstraintForeignKey, table: _ITable) {
        const ftable = asTable(table.ownerSchema.getObject(cst.foreignTable, { beingCreated: table }));
        const cols = cst.localColumns.map(x => table.getColumnRef(x.name));
        const fcols = cst.foreignColumns.map(x => ftable.getColumnRef(x.name));
        this.table = table;
        this.foreignTable = ftable;
        if (cols.length !== fcols.length) {
            throw new QueryError('Foreign key count mismatch');
        }
        cols.forEach((c, i) => {
            if (fcols[i].expression.type !== c.expression.type) {
                throw new QueryError(`Foreign key column type mismatch`);
            }
        });

        if ((cst.match ?? 'simple') !== 'simple' && cols.length !== 1) {
            throw new NotSupported(`matching mode '${cst.match}' on mutliple columns foreign keys`);
        }

        // check that there is an unique index on this table for the given expressions
        const findex = ftable.getIndex(...fcols.map(x => x.expression));
        if (!findex?.unique) {
            throw new QueryError(`there is no unique constraint matching given keys for referenced table "${ftable.name}"`);
        }


        // auto-create indices
        if (this.db.options.autoCreateForeignKeyIndices) {
            table.createIndex(_t, {
                ifNotExists: true,
                columns: cols.map<CreateIndexColDef>(x => ({
                    value: x.expression,
                })),
            });
        }

        // ========================
        // when changing the foreign table key, check correspondances in this table
        // ========================
        const onUpdate = cst.onUpdate ?? 'no action';
        const onDelete = cst.onDelete ?? 'no action';
        this.unsubs.push(ftable.onBeforeChange(cst.foreignColumns.map(x => x.name), (old, neu, dt) => {
            if (!old) {
                return;
            }
            const oVals = fcols.map(x => old[x.expression.id!]);
            if (oVals.some(nullIsh)) {
                return;
            }
            // build foreign key equality expression
            const equals = cst.localColumns.map<ExprBinary>((x, i) => ({
                type: 'binary',
                op: '=',
                left: { type: 'ref', name: x.name, table: { name: table.name } },
                // hack, see #fkcheck
                right: {
                    type: 'constant',
                    value: oVals[i],
                    dataType: fcols[i].expression.type as any, // hack
                },
            }));
            const expr = equals.slice(1).reduce<Expr>((a, b) => ({
                type: 'binary',
                op: 'AND',
                left: a,
                right: b,
            }), equals[0]);

            // check nothing matches
            for (const local of table.selection.filter(expr).enumerate(dt)) {
                // ====== ON DELETE
                switch (neu ? onUpdate : onDelete) {
                    case 'no action':
                    case 'restrict':
                        throw new QueryError(`update or delete on table "${ftable.name}" violates foreign key constraint on table "${this.name}"`);
                    case 'cascade':
                        if (neu) {
                            for (let i = 0; i < fcols.length; i++) {
                                local[cst.localColumns[i].name] = neu[cst.foreignColumns[i].name];
                            }
                            table.update(dt, local);
                        } else {
                            table.delete(dt, local);
                        }
                        break;
                    case 'set default':
                    case 'set null':
                        for (const c of cst.localColumns) {
                            local[c.name] = null;
                        }
                        table.update(dt, local);
                        break;
                }
            }
        }));

        // =====================
        //  when changing something in this table,
        //  then there must be a key match in the foreign table
        // =====================
        this.unsubs.push(table.onBeforeChange(cst.localColumns.map(x => x.name), (_, neu, dt) => {
            if (!neu) {
                return;
            }
            const vals = cols.map(x => (neu as any)[x.expression.id!]);
            if (vals.some(nullIsh)) {
                return;
            }
            // build foreign key equality expression
            const equals = cst.foreignColumns.map<ExprBinary>((x, i) => ({
                type: 'binary',
                op: '=',
                left: { type: 'ref', name: x.name, table: { name: ftable.name } },
                // hack, see #fkcheck
                right: {
                    type: 'constant',
                    value: vals[i],
                    dataType: cols[i].expression.type as any, // hack
                },
            }));
            const expr = equals.slice(1).reduce<Expr>((a, b) => ({
                type: 'binary',
                op: 'AND',
                left: a,
                right: b,
            }), equals[0]);

            // check there is a match
            let yielded = false;
            for (const _ of ftable.selection.filter(expr).enumerate(dt)) {
                yielded = true;
            }
            if (!yielded) {
                throw new QueryError(`insert or update on table "${ftable.name}" violates foreign key constraint on table "${this.name}"`);
            }
        }));


        // =====================
        //  prevent foreign table from being dropped
        // =====================
        this.unsubs.push(ftable.onDrop((t, cascade) => {
            //  (todo implement multiple drops)
            if (cascade) {
                this.uninstall(t);
            } else {
                throw new QueryError({
                    error: `cannot drop table "${ftable.name}" because other objects depend on it`,
                    details: `constraint ${this.name} on table ${table.name} depends on table "${ftable.name}"`,
                    hint: `Use DROP ... CASCADE to drop the dependent objects too.`,
                });
            }
        }));

        // =====================
        //  prevent foreign table truncation
        // =====================
        this.unsubs.push(ftable.onTruncate((t, { cascade }) => {
            if (cascade) {
                this.table.truncate(t, { cascade: true });
                return;
            }
            throw new QueryError({
                error: `cannot truncate a table referenced in a foreign key constraint`,
                details: `Table "${table.name}" references "${ftable.name}".`,
                hint: `HINT:  Truncate table "${table.name}" at the same time, or use TRUNCATE ... CASCADE.`,
            })
        }));

        // =====================
        //  when this table is dropped => remove hooks on foreign table
        // =====================
        table.onDrop(dt => {
            this.uninstall(dt);
        });

        return this;
    }

    uninstall(t: _Transaction): void {
        this.unsubs.forEach(x => x.unsubscribe());
        this.unsubs = [];
    }
}