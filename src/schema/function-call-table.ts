import { _Transaction, IValue, _Explainer, _IIndex, _SelectExplanation, Stats } from '../interfaces-private';
import { RecordCol } from '../datatypes';
import { buildCtx } from '../parser/context';
import { DataSourceBase } from '../transforms/transform-base';
import { columnEvaluator } from '../transforms/selection';
import { colByName, fromEntries } from '../utils';

export class FunctionCallTable extends DataSourceBase<any> {
    readonly columns: readonly IValue<any>[];
    private readonly colsByName: Map<string, IValue<any>>;
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

    getColumn(column: string, nullIfNotFound?: boolean | undefined): IValue<any> {
        return colByName(this.colsByName, column, nullIfNotFound)!;
    }

    getIndex(forValue: IValue<any>): _IIndex<any> | null | undefined {
        return null;
    }

    isOriginOf(value: IValue<any>): boolean {
        return value.origin === this;
    }


    explain(e: _Explainer): _SelectExplanation {
        throw new Error('Method not implemented.');
    }

    stats(t: _Transaction): Stats | null {
        throw new Error('Method not implemented.');
    }
}
