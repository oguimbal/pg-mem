import { TransformBase } from './transform-base';
import { columnEvaluator } from './selection';
import { IValue, _ISelection, _Transaction, _IIndex, _Explainer, _SelectExplanation, Stats, Row, setId, getId } from '../interfaces-private';
import { ExprRef, nil } from 'pgsql-ast-parser';
import { ArrayType } from '../datatypes';

// A set-returning function call in FROM whose arguments reference preceding FROM items
// (postgres treats those as implicitly LATERAL): the set is re-evaluated for each row
// of the left side, cross-joining the row with each element.
//   ex: select id, tag from users, unnest(users.tags) as tag

let latCnt = 0;

export class LateralCallTable extends TransformBase {
    readonly columns: readonly IValue[];
    private readonly elemCol: IValue;
    private readonly mapping = new Map<IValue, IValue>();
    private readonly symbol = Symbol();
    private readonly latId = latCnt++;

    get isExecutionWithNoResult(): boolean {
        return false;
    }

    constructor(base: _ISelection
        , private value: IValue
        , private colName: string) {
        super(base);
        const wrapped = base.columns.map(c => {
            const nc = c.setWrapper(this, x => (x as any)['>left']);
            this.mapping.set(c, nc);
            return nc;
        });
        this.elemCol = columnEvaluator(this, colName, (value.type as ArrayType).of).setOrigin(this);
        this.columns = [...wrapped, this.elemCol];
    }

    entropy(t: _Transaction): number {
        // each left row is assumed to fan out into a couple of elements
        return this.base.entropy(t) * 2;
    }

    stats(): Stats | null {
        return null;
    }

    *enumerate(t: _Transaction): Iterable<Row> {
        for (const l of this.base.enumerate(t)) {
            const arr = this.value.get(l, t);
            if (!Array.isArray(arr)) {
                // null set: cross join lateral drops the row
                continue;
            }
            for (let i = 0; i < arr.length; i++) {
                const row: any = {
                    '>left': l,
                    [this.colName]: arr[i],
                    [this.symbol]: true,
                };
                yield setId(row, `lat${this.latId}_${getId(l)}_${i}`);
            }
        }
    }

    hasItem(value: Row): boolean {
        return !!(value as any)[this.symbol];
    }

    getColumn(column: string | ExprRef): IValue;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil;
    getColumn(column: string | ExprRef, nullIfNotFound?: boolean): IValue | nil {
        const name = typeof column === 'string' ? column : column.name;
        const table = typeof column === 'string' ? null : column.table?.name;
        if (name === this.colName && (!table || table === this.colName)) {
            return this.elemCol;
        }
        const got = this.base.getColumn(column, nullIfNotFound);
        return got && (this.mapping.get(got) ?? got);
    }

    getIndex(): _IIndex | nil {
        return null;
    }

    isOriginOf(a: IValue): boolean {
        return a.origin === this || this.base.isOriginOf(a);
    }

    explain(e: _Explainer): _SelectExplanation {
        return {
            _: 'map',
            id: e.idFor(this),
            select: [],
            of: this.base.explain(e),
        };
    }
}
