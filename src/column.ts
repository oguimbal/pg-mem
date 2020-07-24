import { _Column, IValue, _IIndex, NotSupported, _Transaction, QueryError, _IType } from './interfaces-private';
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
        , public expression: Evaluator) {
    }

    addConstraint(constraint: ColumnConstraint, t: _Transaction): this {
        switch (constraint.type) {
            case 'primary key':
                this.table.createIndex(t, {
                    columns: [{ value: this.expression }],
                    primary: true,
                });
                break;
            case 'unique':
                this.table.createIndex(t, {
                    columns: [{ value: this.expression }],
                    notNull: constraint.notNull,
                    unique: true,
                });
                break;
            case 'not null':
                this.addNotNullConstraint(t);
                break;
            default:
                throw NotSupported.never(constraint, 'add constraint type');
        }
        return this;
    }


    private addNotNullConstraint(t: _Transaction) {// check has no null value
        const bin = this.table.bin(t);
        for (const e of bin.values()) {
            const val = this.expression.get(e, t);
            if (nullIsh(val)) {
                throw new QueryError(`Cannot add not null constraint on column "${this.expression.id}": it contains null values`);
            }
        }
        this.notNull = true;
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
        return this;
    }

    alter(alter: AlterColumn, t: _Transaction): this {
        switch (alter.type) {
            case 'drop default':
                this.default = null;
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
                break;
            case 'set not null':
                this.addNotNullConstraint(t);
                break;
            case 'drop not null':
                this.notNull = false;
                break;
            case 'set type':
                const newType = fromNative(alter.dataType);
                const conv = this.expression.convert(newType);
                const eid = this.expression.id;

                this.table.remapData(t, x => x[this.expression.id] = conv.get(x, t));

                // once converted, do nasty things to change expression
                this.replaceExpression(eid, newType);
                break;
            default:
                throw NotSupported.never(alter, 'alter column type');
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
        if (i < 0) {
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