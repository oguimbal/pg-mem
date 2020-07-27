import { _Column, IValue, _IIndex, NotSupported, _Transaction, QueryError, _IType, SchemaField } from './interfaces-private';
import type { MemoryTable } from './table';
import { Evaluator } from './valuetypes';
import { ColumnConstraint, AlterColumn } from './parser/syntax/ast';
import { nullIsh } from './utils';
import { buildValue } from './predicate';
import { fromNative } from './datatypes';
import { columnEvaluator } from './transforms/selection';



export class ColRef implements _Column {

    default: IValue;
    notNull: boolean;
    usedInIndexes = new Set<_IIndex>();

    constructor(private table: MemoryTable
        , public expression: Evaluator
        , private _schema: SchemaField) {
    }

    addConstraint(constraint: ColumnConstraint, t: _Transaction, noAmendSchema?: boolean): this {
        switch (constraint.type) {
            case 'primary key':
                this.table.createIndex(t, {
                    columns: [{ value: this.expression }],
                    primary: true,
                });
                if (!noAmendSchema) {
                    switch (this._schema.constraint?.type) {
                        case 'primary key':
                        case 'not null':
                        case 'unique':
                        case null:
                        case undefined:
                            this._schema.constraint = constraint;
                            break;
                        default:
                            throw NotSupported.never(this._schema.constraint);
                    }
                }
                break;
            case 'unique':
                this.table.createIndex(t, {
                    columns: [{ value: this.expression }],
                    notNull: constraint.notNull,
                    unique: true,
                });
                if (!noAmendSchema) {
                    switch (this._schema.constraint?.type) {
                        case 'primary key':
                            break; // ignore (shouldnot ?)
                        case 'not null':
                        case 'unique':
                        case null:
                        case undefined:
                            this._schema.constraint = constraint;
                            break;
                        default:
                            throw NotSupported.never(this._schema.constraint);
                    }
                }
                break;
            case 'not null':
                if (!noAmendSchema) {
                    switch (this._schema.constraint?.type) {
                        case 'primary key':
                            break; // ignore (shouldnot ?)
                        case 'unique':
                            this._schema.constraint.notNull = true;
                            break;
                        case 'not null':
                        case null:
                        case undefined:
                            this._schema.constraint = constraint;
                            break;
                        default:
                            throw NotSupported.never(this._schema.constraint);
                    }
                }
                this.addNotNullConstraint(t, noAmendSchema);
                break;
            default:
                throw NotSupported.never(constraint, 'add constraint type');
        }
        if (!noAmendSchema) {
            this.table.schema.db.onSchemaChange();
        }
        return this;
    }


    private addNotNullConstraint(t: _Transaction, noAmendSchema?: boolean) {// check has no null value
        const bin = this.table.bin(t);
        for (const e of bin.values()) {
            const val = this.expression.get(e, t);
            if (nullIsh(val)) {
                throw new QueryError(`Cannot add not null constraint on column "${this.expression.id}": it contains null values`);
            }
        }
        this.notNull = true;

        // just amend schema (for cloning)
        if (!noAmendSchema) {
            if (!this._schema.constraint) {
                this._schema.constraint = { type: 'not null' };
            } else {
                switch (this._schema.constraint.type) {
                    case 'not null':
                    case 'primary key':
                        break; // already not null
                    case 'unique':
                        this._schema.constraint.notNull = true;
                        break;
                    default:
                        throw NotSupported.never(this._schema.constraint);
                }
            }
            this.table.schema.db.onSchemaChange();
        }
    }

    rename(to: string, t: _Transaction): this {
        if (this.table.getColumnRef(to, true)) {
            throw new QueryError(`Column "${to}" already exists`);
        }

        // first, move data (this cannot throw => OK to modify mutable data)
        this.table.remapData(t, v => {
            const ov = v[this.expression.id];
            delete v[this.expression.id];
            v[to] = ov;
        });
        // for (const v of this.table.bin(t)) {
        //     const ov = v[this.expression.id];
        //     delete v[this.expression.id];
        //     v[to] = ov;
        // }

        // === do nasty things to rename column
        this.replaceExpression(to, this.expression.type);
        this._schema.name = to;
        this.table.schema.db.onSchemaChange();
        return this;
    }

    alter(alter: AlterColumn, t: _Transaction, noAmendSchema?: boolean): this {
        switch (alter.type) {
            case 'drop default':
                this.default = null;
                if (!noAmendSchema) {
                    this._schema.default = null;
                }
                break;
            case 'set default':
                const df = buildValue(this.table.selection, alter.default);
                if (!df.isConstant) {
                    throw new QueryError('cannot use column references in default expression');
                }
                if (alter.updateExisting) {
                    const defVal = df.get();
                    this.table.remapData(t, x => x[this.expression.id] = defVal);
                }
                this.default = df;
                if (!noAmendSchema) {
                    this._schema.default = alter.default;
                }
                break;
            case 'set not null':
                this.addNotNullConstraint(t, noAmendSchema);
                break;
            case 'drop not null':
                if (this._schema.constraint) {
                    switch (this._schema.constraint.type) {
                        case 'not null':
                            this._schema.constraint = null;
                            break;
                        case 'primary key':
                            throw new QueryError('Cannot drop not null constraint when constraint is a primary key');
                        case 'unique':
                            this._schema.constraint.notNull = false;
                            break;
                        default:
                            throw NotSupported.never(this._schema.constraint);
                    }
                }
                this.notNull = false;
                break;
            case 'set type':
                const newType = fromNative(alter.dataType);
                const conv = this.expression.convert(newType);
                const eid = this.expression.id;

                this.table.remapData(t, x => x[this.expression.id] = conv.get(x, t));

                // once converted, do nasty things to change expression
                this.replaceExpression(eid, newType);
                if (!noAmendSchema) {
                    this._schema.type = newType;
                }
                break;
            default:
                throw NotSupported.never(alter, 'alter column type');
        }
        if (!noAmendSchema) {
            this.table.schema.db.onSchemaChange();
        }
        return this;
    }

    private replaceExpression(newId: string, newType: _IType) {
        const on = this.expression.id.toLowerCase();
        const nn = newId.toLowerCase();
        this.expression = columnEvaluator(this.table, newId, newType);

        // replace in table
        this.table.columnsByName.delete(on);
        this.table.columnsByName.set(nn, this);
    }

    drop(t: _Transaction): void {
        const on = this.expression.id.toLowerCase();
        const i = this.table.columnDefs.indexOf(this);
        const ii = this.table._schema.fields.indexOf(this._schema);
        if (i < 0 || ii !== i) {
            throw new Error('Corrupted table');
        }

        // remove indices
        for (const u of this.usedInIndexes) {
            this.table.dropIndex(u);
        }

        // remove associated data
        this.table.remapData(t, x => delete x[this.expression.id]);

        // nasty business to remove columns
        this.table.columnsByName.delete(on);
        this.table.columnDefs.splice(i, 1);
        this.table._schema.fields.splice(i, 1);
        this.table.schema.db.onSchemaChange();
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
        if (!this.default) {
            return;
        }
        const col = this.expression.get(toInsert, t);
        if (!nullIsh(col)) {
            return;
        }
        toInsert[this.expression.id] = this.default.get();
    }
}