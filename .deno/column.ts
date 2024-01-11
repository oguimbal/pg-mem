import { _Column, IValue, _IIndex, NotSupported, _Transaction, QueryError, _IType, SchemaField, ChangeHandler, nil, ISubscription, DropHandler } from './interfaces-private.ts';
import type { MemoryTable } from './table.ts';
import { Evaluator } from './evaluator.ts';
import { ColumnConstraint, AlterColumn } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { nullIsh } from './utils.ts';
import { columnEvaluator } from './transforms/selection.ts';
import { BIndex } from './schema/btree-index.ts';
import { GeneratedIdentityConstraint } from './constraints/generated.ts';
import { buildValue } from './parser/expression-builder.ts';
import { withSelection } from './parser/context.ts';



export class ColRef implements _Column {

    default: IValue | nil;
    notNull = false;
    usedInIndexes = new Set<BIndex>();
    private drophandlers = new Set<DropHandler>();

    constructor(readonly table: MemoryTable
        , public expression: Evaluator
        , _schema: SchemaField
        , public name: string) {
    }

    addConstraints(clist: ColumnConstraint[], t: _Transaction): this {
        const notNull = clist.some(x => x.type === 'not null' || x.type === 'primary key');
        const acceptNil = clist.some(x => x.type === 'null');
        if (notNull && acceptNil) {
            throw new QueryError(`conflicting NULL/NOT NULL declarations for column "${this.name}" of table "${this.table.name}"`)
        }
        for (const c of clist) {
            const cname = c.constraintName;
            switch (c.type) {
                case 'not null':
                case 'null':
                    // dealt with that above.
                    break;
                case 'primary key':
                    this.table.createIndex(t, {
                        columns: [{ value: this.expression }],
                        primary: true,
                        // default constraint name:
                        indexName: cname?.name ?? `${this.table.name}_pkey`,
                    });
                    break;
                case 'unique':
                    this.table.createIndex(t, {
                        columns: [{ value: this.expression }],
                        notNull: notNull,
                        unique: true,
                        // default constraint name:
                        indexName: cname?.name ?? `${this.table.name}_${this.name}_key`,
                    });
                    break;
                case 'default':
                    this.alter({
                        type: 'set default',
                        default: c.default,
                        updateExisting: true,
                    }, t);
                    break;
                case 'check':
                    this.table.addCheck(t, c.expr, cname?.name);
                    break;
                case 'add generated':
                    new GeneratedIdentityConstraint(c.constraintName?.name, this)
                        .install(t, c);
                    break;
                case 'reference':
                    this.table.addForeignKey({
                        ...c,
                        type: 'foreign key',
                        localColumns: [{ name: this.name }],
                    }, t);
                    break;
                default:
                    throw NotSupported.never(c, 'add constraint type');
            }
        }
        if (notNull) {
            this.addNotNullConstraint(t);
        }
        this.table.db.onSchemaChange();
        return this;
    }


    private addNotNullConstraint(t: _Transaction) {
        // check has no null value
        const bin = this.table.bin(t);
        for (const e of bin.values()) {
            const val = this.expression.get(e, t);
            if (nullIsh(val)) {
                throw new QueryError(`Cannot add not null constraint on column "${this.expression.id}": it contains null values`);
            }
        }
        this.notNull = true;

        // just amend schema (for cloning)
        this.table.db.onSchemaChange();
    }

    rename(to: string, t: _Transaction): this {
        if (this.table.getColumnRef(to, true)) {
            throw new QueryError(`Column "${to}" already exists`);
        }

        // first, move data (this cannot throw => OK to modify mutable data)
        this.table.remapData(t, v => {
            const ov = v[this.expression.id!];
            delete v[this.expression.id!];
            v[to] = ov;
        });
        // for (const v of this.table.bin(t)) {
        //     const ov = v[this.expression.id];
        //     delete v[this.expression.id];
        //     v[to] = ov;
        // }

        // === do nasty things to rename column
        this.replaceExpression(to, this.expression.type);
        this.table.db.onSchemaChange();
        this.table.selection.rebuild();
        this.name = to;
        return this;
    }

    alter(alter: AlterColumn, t: _Transaction): this {
        switch (alter.type) {
            case 'drop default':
                this.default = null;
                break;
            case 'set default':
                if (alter.default.type === 'null') {
                    this.default = null;
                    break;
                }
                const df = withSelection(this.table.selection, () => buildValue(alter.default));
                if (!df.isConstant) {
                    throw new QueryError('cannot use column references in default expression');
                }
                if (alter.updateExisting) {
                    const defVal = df.get();
                    this.table.remapData(t, x => x[this.expression.id!] = defVal);
                }
                this.default = df;
                break;
            case 'set not null':
                this.addNotNullConstraint(t);
                break;
            case 'drop not null':
                this.notNull = false;
                break;
            case 'set type':
                const newType = this.table.ownerSchema.getType(alter.dataType);
                const conv = this.expression.cast(newType);
                const eid = this.expression.id;

                this.table.remapData(t, x => x[this.expression.id!] = conv.get(x, t));

                // once converted, do nasty things to change expression
                this.replaceExpression(eid!, newType);
                break;
            case 'add generated':
                new GeneratedIdentityConstraint(alter.constraintName?.name, this)
                    .install(t, alter);
                break;
            default:
                throw NotSupported.never(alter, 'alter column type');
        }
        this.table.db.onSchemaChange();
        this.table.selection.rebuild();
        return this;
    }

    private replaceExpression(newId: string, newType: _IType) {
        const on = this.expression.id!;
        const nn = newId;
        this.expression = columnEvaluator(this.table, newId, newType);

        // replace in table
        this.table.columnMgr.delete(on);
        this.table.columnMgr.set(nn, this);
    }

    drop(t: _Transaction): void {
        const on = this.expression.id!;
        if (!this.table.columnMgr.has(on)) {
            throw new Error('Corrupted table');
        }

        // remove indices
        for (const u of this.usedInIndexes) {
            this.table.dropIndex(t, u.name);
        }

        // remove associated data
        this.table.remapData(t, x => delete x[this.expression.id!]);

        // nasty business to remove columns
        this.table.columnMgr.delete(on);
        this.table.selection.rebuild();
        this.drophandlers.forEach(d => d(t, false));
        this.table.db.onSchemaChange();
    }

    checkConstraints(toInsert: any, t: _Transaction) {
        if (!this.notNull) {
            return;
        }
        const col = this.expression.get(toInsert, t);
        if (nullIsh(col)) {
            throw new QueryError(`null value in column "${this.expression.id}" violates not-null constraint`);
        }
    }

    setDefaults(toInsert: any, t: _Transaction) {
        const col = this.expression.get(toInsert, t);
        if (col !== undefined) {
            return;
        }
        if (!this.default) {
            toInsert[this.expression.id!] = null;
        } else {
            toInsert[this.expression.id!] = this.default.get();
        }
    }


    onDrop(sub: DropHandler): ISubscription {
        this.drophandlers.add(sub);
        return {
            unsubscribe: () => {
                this.drophandlers.delete(sub);
            }
        }
    }

}
