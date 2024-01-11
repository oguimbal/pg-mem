import { DataSourceBase } from '../../transforms/transform-base.ts';
import { ArrayFilter } from '../../transforms/array-filter.ts';
import { cleanResults } from '../clean-results.ts';
import { _ISelection, _ISchema, _ITable, _Transaction, IValue, _IIndex, _Explainer, _IStatement, QueryError, _Column } from '../../interfaces-private.ts';
import { InsertStatement, UpdateStatement, DeleteStatement, SetStatement, ExprRef } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { buildSelection } from '../../transforms/selection.ts';
import { MemoryTable } from '../../table.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { withSelection, buildCtx } from '../../parser/context.ts';
import { colToStr } from '../../utils.ts';

type MutationStatement = InsertStatement | UpdateStatement | DeleteStatement;


export abstract class MutationDataSourceBase<T> extends DataSourceBase<T> {
    public static readonly affectedRows = Symbol('affectedRows');

    /** Perform the mutation, and returns the affected elements */
    protected abstract performMutation(t: _Transaction): T[];

    private returningRows?: ArrayFilter;
    private returning?: _ISelection;
    private mutationResult = Symbol('mutationResult');

    get isExecutionWithNoResult(): boolean {
        return !this.returning;
    }

    isAggregation() {
        return false;
    }

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

        const { onFinishExecution } = buildCtx();
        // force execution if it has not yet been executed once the current statement finishes its execution.
        // see "only inserts once with statement is executed" test
        onFinishExecution(t => {
            this._doExecuteOnce(t);
        });
    }

    private _doExecuteOnce(t: _Transaction): any[] {
        // check if this mutation has already been executed in the statement being executed
        // and get the result from cache to avoid re-excuting it
        // see unit test "can use delete result multiple times in select"
        let affected = t.getTransient<T[]>(this.mutationResult);
        if (!affected) {
            // execute mutation if nescessary
            affected = this.performMutation(t);
            t.setTransient(this.mutationResult, affected);
        }


        // set the result count
        t.setTransient(MutationDataSourceBase.affectedRows, affected.length);
        return affected;
    }

    *enumerate(t: _Transaction): Iterable<any> {

        const affected = this._doExecuteOnce(t);

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

    getColumn(column: string | ExprRef, nullIfNotFound?: boolean | undefined): IValue<any> {
        if (!this.returning) {
            throw new Error(`Cannot get column "${colToStr(column)}" from a mutation that has no returning statement`);
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
    return withSelection(setSelection, () => {
        const alreadySet = new Set<_Column>();
        const sets = _sets.map(x => {
            const col = (setTable as MemoryTable).getColumnRef(x.column.name);
            if (alreadySet.has(col)) {
                throw new QueryError(` multiple assignments to same column "${col.name}"`, '42601');
            }
            alreadySet.add(col);
            return {
                col,
                value: x.value,
                getter: x.value.type !== 'default'
                    ? buildValue(x.value).cast(col.expression.type)
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
    });
}
