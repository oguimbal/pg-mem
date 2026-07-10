import { DataSourceBase } from '../transforms/transform-base.ts';
import { columnEvaluator } from '../transforms/selection.ts';
import { IValue, _ISelection, _Transaction, _IIndex, _Explainer, _SelectExplanation, Stats, Row, setId, _IType } from '../interfaces-private.ts';
import { buildCtx } from '../parser/context.ts';
import { colByName, fromEntries } from '../utils.ts';
import hash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';

// WITH RECURSIVE evaluation, postgres-style:
// the seed term populates a working set; the recursive term is evaluated against the
// working set (not the accumulated result) over and over, each round's output becoming
// the next working set, until a round yields nothing. UNION (without ALL) drops rows
// already produced, which is also what makes cyclic recursions terminate.

export interface CteCol {
    name: string;
    type: _IType;
}

let rcteCnt = 0;

abstract class CteSourceBase extends DataSourceBase {
    readonly columns: readonly IValue[];
    private readonly colsByName: Map<string, IValue>;
    protected readonly symbol = Symbol();

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    constructor(protected cols: CteCol[]) {
        super(buildCtx().schema);
        this.columns = cols.map(c => columnEvaluator(this, c.name, c.type).setOrigin(this));
        this.colsByName = fromEntries(this.columns.map(c => [c.id!, c]));
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

    hasItem(v: Row): boolean {
        return !!(v as any)[this.symbol];
    }

    stats(): Stats | null {
        return null;
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'constantSet',
            id: e.idFor(this),
            rawArrayLen: 0,
        };
    }
}

type nil = null | undefined;

/** The "working table" the recursive term reads: rows of the previous iteration only */
export class RecursiveCteBuffer extends CteSourceBase {
    rows: any[] = [];

    entropy(): number {
        return this.rows.length;
    }

    *enumerate(): Iterable<Row> {
        for (const r of this.rows) {
            r[this.symbol] = true;
            yield r;
        }
    }
}

export class RecursiveCte extends CteSourceBase {
    private recursiveSel!: _ISelection;
    private readonly rcteId = rcteCnt++;

    constructor(cols: CteCol[]
        , private seed: _ISelection
        , private buffer: RecursiveCteBuffer
        , private dedup: boolean) {
        super(cols);
    }

    setRecursive(sel: _ISelection): void {
        this.recursiveSel = sel;
    }

    entropy(t: _Transaction): number {
        // unknowable before evaluating: assume each row recurses once
        return this.seed.entropy(t) * 2;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        const seen = this.dedup ? new Set<string>() : null;
        let idCnt = 0;
        const project = (raw: any, from: _ISelection): any | null => {
            const row: any = {};
            this.cols.forEach((c, i) => row[c.name] = from.columns[i].get(raw, t));
            if (seen) {
                const h = hash(this.cols.map(c => row[c.name]));
                if (seen.has(h)) {
                    return null;
                }
                seen.add(h);
            }
            row[this.symbol] = true;
            return setId(row, `rcte${this.rcteId}_${idCnt++}`);
        };

        let working: any[] = [];
        for (const raw of this.seed.enumerate(t)) {
            const row = project(raw, this.seed);
            if (row) {
                working.push(row);
            }
        }
        yield* working;

        while (working.length) {
            this.buffer.rows = working;
            const next: any[] = [];
            for (const raw of this.recursiveSel.enumerate(t)) {
                const row = project(raw, this.recursiveSel);
                if (row) {
                    next.push(row);
                }
            }
            yield* next;
            working = next;
        }
        this.buffer.rows = [];
    }
}
