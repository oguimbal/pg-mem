import { DataSourceBase } from '../transforms/transform-base';
import { ArrayFilter } from '../transforms/array-filter';
import { cleanResults } from '../clean-results';
import { _ISelection, _ISchema, _ITable, _Transaction, IValue, _IIndex, _Explainer } from '../interfaces-private';
import { InsertStatement, UpdateStatement, DeleteStatement, SetStatement } from 'pgsql-ast-parser';
import { buildSelection } from '../transforms/selection';
import { MemoryTable } from '../table';
import { buildValue } from '../expression-builder';

type MutationStatement = InsertStatement | UpdateStatement | DeleteStatement;

export abstract class MutationDataSourceBase<T> extends DataSourceBase<T> {

    /** Perform the mutation, and returns the affected elements */
    protected abstract performMutation(t: _Transaction): T[];

    private returningRows?: ArrayFilter;
    private returning?: _ISelection;

    get columns() {
        return this.returning?.columns ?? [];
    }

    constructor(protected table: _ITable, protected mutatedSel: _ISelection, p: MutationStatement) {
        super(table.ownerSchema);

        // prepare "returning" statement
        if (p.returning) {
            this.returningRows = new ArrayFilter(this.mutatedSel, [])
            this.returning = buildSelection(this.returningRows, p.returning);
        }
    }

    *enumerate(t: _Transaction): Iterable<any> {
        const affected = this.performMutation(t);

        t.affectedRows = affected.length;
        if (!this.returning) {
            return;
        }

        // handle "returning" statement
        try {
            cleanResults(affected);
            this.returningRows!.rows = affected;
            yield* this.returning.enumerate(t);
        } finally {
            this.returningRows!.rows = []
        }
    }

    entropy(t: _Transaction): number {
        // To ensure that a muation will always be prioritary
        //  on a join, then just return 0.
        return 0;
    }

    getColumn(column: string, nullIfNotFound?: boolean | undefined): IValue<any> {
        if (!this.returning) {
            throw new Error('Cannot get column from a mutation that has no returning statement');
        }
        return this.returning.getColumn(column, nullIfNotFound)!;
    }

    hasItem(value: any, t: _Transaction): boolean {
        throw new Error('To fix: Joins cannot call hasItem on a mutation');
    }

    getIndex(forValue: IValue<any>): _IIndex<any> | null | undefined {
        return null;
    }

    explain(e: _Explainer): never {
        throw new Error('not implemented');
    }

    isOriginOf(a: IValue<any>): boolean {
        return !!this.returning && a.origin === this.returning;
    }

    stats(t: _Transaction): null {
        return null;
    }
}


export type Setter = (t: _Transaction, target: any, source: any) => void;
export function createSetter(this: void, setTable: _ITable, setSelection: _ISelection, _sets: SetStatement[]): Setter {

    const sets = _sets.map(x => {
        const col = (setTable as MemoryTable).getColumnRef(x.column.name);
        return {
            col,
            value: x.value,
            getter: x.value.type !== 'default'
                ? buildValue(setSelection, x.value).cast(col.expression.type)
                : null,
        };
    });

    return (t: _Transaction, target: any, source: any) => {
        for (const s of sets) {
            if (s.value.type === 'default') {
                target[s.col.expression.id!] = s.col.default?.get() ?? undefined;
            } else {
                target[s.col.expression.id!] = s.getter?.get(source, t) ?? null;
            }
        }
    }
}
