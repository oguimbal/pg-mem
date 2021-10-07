import { nil, QueryError, Schema, SchemaField } from '../interfaces';
import { IValue, setId, _ISchema, _IType, _Transaction } from '../interfaces-private';
import { Expr } from 'pgsql-ast-parser';
import { ReadOnlyTable } from './readonly-table';
import { buildValue } from '../expression-builder';
import { Types } from '../datatypes';

let cnt = 0;
export class ValuesTable extends ReadOnlyTable {
    private items: any[];
    private symbol = Symbol();

    entropy(t: _Transaction): number {
        return this.items.length;
    }
    enumerate(t: _Transaction): Iterable<any> {
        return this.items;
    }
    hasItem(value: any, t: _Transaction): boolean {
        return !!value[this.symbol];
    }


    readonly _schema: Schema;

    constructor(owner: _ISchema, alias: string, items: Expr[][], columnNames: string[] | nil, acceptDefault?: boolean) {
        super(owner);
        const len = new Set(items.map(x => x.length));
        if (len.size !== 1) {
            throw new QueryError('VALUES lists must all be the same length');
        }
        if (columnNames && columnNames.length > items[0].length) {
            throw new QueryError(`table "${alias}" has ${items[0].length} columns available but ${columnNames.length} columns specified`);
        }
        type V = IValue | 'default';
        let builtVals: V[][] = items.map(vals => vals.map(e => {
            if (acceptDefault && e.type === 'default') {
                return 'default';
            }
            return buildValue(owner.dualTable.selection, e);
        }));
        const types = items[0].map((_, i) => {
            return preferedType(builtVals.map(x => {
                const v = x[i];
                return v === 'default'
                    ? null
                    : v.type;
            }))
        });
        this._schema = {
            name: alias,
            fields: types.map<SchemaField>((type, i) => {
                return {
                    type: type ?? Types.default,
                    name: columnNames?.[i] ?? `column${i + 1}`,
                }
            })
        };
        this.items = builtVals.map(vals => {
            const ret = { [this.symbol]: true } as any;
            setId(ret, 'vtbl' + (++cnt));
            for (let i = 0; i < vals.length; i++) {
                const v = vals[i];
                ret[this._schema.fields[i].name] = v === 'default'
                    ? null
                    : v.cast(types[i]!).get();
            }
            return ret;
        });
    }
}

function preferedType(types: (_IType | null)[]) {
    return types.reduce((a, b) => {
        if (!a) {
            return b;
        }
        if (!b) {
            return a;
        }
        const ret = a.prefer(b);
        if (!ret) {
            throw new QueryError('Incompatible value types');
        }
        return ret;
    }, types[0]);
}