import { DataSourceBase } from './transform-base.ts';
import { columnEvaluator, Selection } from './selection.ts';
import { IValue, _ISelection, _Transaction, _IIndex, _Explainer, _SelectExplanation, Stats, Row, setId, getId } from '../interfaces-private.ts';
import { buildCtx } from '../parser/context.ts';
import { ArrayType } from '../datatypes/index.ts';
import { colByName, fromEntries } from '../utils.ts';
import { nil } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';

// Set-returning functions in a select list (select unnest(arr), generate_series(..))
// expand the projected rows: one output row per element. Multiple SRFs advance in
// lockstep, padded with nulls to the longest (pg >= 10 semantics); rows where every
// SRF yields nothing vanish.

const srfValues = new WeakSet<IValue>();

export function markSetReturning<T extends IValue>(v: T): T {
    srfValues.add(v);
    return v;
}

export function isSetReturning(v: IValue): boolean {
    return srfValues.has(v);
}

export function expandSrfs(sel: _ISelection): _ISelection {
    if (!(sel instanceof Selection) || !sel.originValues.some(isSetReturning)) {
        return sel;
    }
    return new ExpandedSelection(sel);
}

class ExpandedSelection extends DataSourceBase {
    readonly columns: readonly IValue[];
    private readonly colsByName: Map<string, IValue>;
    private readonly srfIds: string[] = [];
    private readonly symbol = Symbol();

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    constructor(private sel: Selection) {
        super(buildCtx().schema);
        this.columns = sel.columns.map((c, i) => {
            const origin = sel.originValues[i];
            if (isSetReturning(origin) && origin.type instanceof ArrayType) {
                this.srfIds.push(c.id!);
                return columnEvaluator(this, c.id!, origin.type.of).setOrigin(this);
            }
            return columnEvaluator(this, c.id!, c.type).setOrigin(this);
        });
        this.colsByName = fromEntries(this.columns.map(c => [c.id!, c]));
    }

    entropy(t: _Transaction): number {
        return this.sel.entropy(t) * 2;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        for (const row of this.sel.enumerate(t)) {
            const arrays = this.srfIds.map(id => (row as any)[id]);
            const count = Math.max(...arrays.map(a => Array.isArray(a) ? a.length : 0));
            for (let k = 0; k < count; k++) {
                const out: any = {};
                for (const c of this.sel.columns) {
                    out[c.id!] = (row as any)[c.id!];
                }
                this.srfIds.forEach((id, i) => {
                    const arr = arrays[i];
                    out[id] = Array.isArray(arr) ? arr[k] ?? null : null;
                });
                out[this.symbol] = true;
                yield setId(out, `${getId(row)}_srf${k}`);
            }
        }
    }

    hasItem(value: Row): boolean {
        return !!(value as any)[this.symbol];
    }

    getColumn(column: string, nullIfNotFound?: boolean): IValue {
        return colByName(this.colsByName, column, nullIfNotFound)!;
    }

    getIndex(): _IIndex | nil {
        return null;
    }

    isOriginOf(v: IValue): boolean {
        return v.origin === this;
    }

    stats(): Stats | null {
        return null;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'map',
            id: e.idFor(this),
            select: [],
            of: this.sel.explain(e),
        };
    }
}
