import { _Transaction, IValue, _Explainer, _IIndex, _SelectExplanation, Stats } from '../interfaces-private.ts';
import { RecordCol } from '../datatypes/index.ts';
import { buildCtx } from '../parser/context.ts';
import { DataSourceBase } from '../transforms/transform-base.ts';
import { columnEvaluator } from '../transforms/selection.ts';
import { colByName, fromEntries } from '../utils.ts';

export class FunctionCallTable extends DataSourceBase {
    readonly columns: readonly IValue[];
    private readonly colsByName: Map<string, IValue>;
    private symbol = Symbol();

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    constructor(cols: readonly RecordCol[], private evaluator: IValue) {
        super(buildCtx().schema);
        this.columns = cols.map(c => columnEvaluator(this, c.name, c.type).setOrigin(this));
        this.colsByName = fromEntries(this.columns.map(c => [c.id!, c]));
    }

    entropy(t: _Transaction): number {
        return 0;
    }

    enumerate(t: _Transaction): Iterable<any> {
        const results = this.evaluator.get(null, t);
        for (const result of results ?? []) {
            result[this.symbol] = true;
        }
        return results;
    }

    hasItem(value: any, t: _Transaction): boolean {
        return !!(value as any)[this.symbol];
    }

    getColumn(column: string, nullIfNotFound?: boolean | undefined): IValue {
        return colByName(this.colsByName, column, nullIfNotFound)!;
    }

    getIndex(forValue: IValue): _IIndex | null | undefined {
        return null;
    }

    isOriginOf(value: IValue): boolean {
        return value.origin === this;
    }


    explain(e: _Explainer): _SelectExplanation {
        throw new Error('Method not implemented.');
    }

    stats(t: _Transaction): Stats | null {
        throw new Error('Method not implemented.');
    }
}
