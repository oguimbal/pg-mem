import { nil, QueryError, Schema, SchemaField } from '../interfaces.ts';
import { IValue, setId, _ISchema, _IType, _Transaction } from '../interfaces-private.ts';
import { Expr } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ReadOnlyTable } from './readonly-table.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { Types } from '../datatypes/index.ts';
import { withSelection, buildCtx } from '../parser/context.ts';

let cnt = 0;
export class ValuesTable extends ReadOnlyTable {
    private symbol = Symbol();
    _schema!: Schema;
    private assignments!: (IValue<any> | "default")[][];

    entropy(t: _Transaction): number {
        return 0;
    }

    enumerate(t: _Transaction): Iterable<any> {
        const ret = this.assignments.map(vals => {
            const ret = { [this.symbol]: true } as any;
            setId(ret, 'vtbl' + (++cnt));
            for (let i = 0; i < vals.length; i++) {
                const v = vals[i];
                ret[this._schema.fields[i].name] = v === 'default'
                    ? null
                    : v.get({}, t);
            }
            return ret;
        });
        return ret;
    }

    hasItem(value: any, t: _Transaction): boolean {
        return !!value[this.symbol];
    }

    constructor(alias: string, items: Expr[][], columnNames: string[] | nil, acceptDefault?: boolean) {
        super(buildCtx().schema);
        withSelection(buildCtx().schema.dualTable.selection, () => {
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
                return buildValue(e);
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
            this.assignments = builtVals.map(vals => vals.map((v, i) => v === 'default' ? v : v.cast(types[i]!)))
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