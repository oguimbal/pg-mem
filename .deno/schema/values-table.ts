import { QueryError, Schema, SchemaField } from '../interfaces.ts';
import { setId, _ISchema, _IType, _Transaction } from '../interfaces-private.ts';
import { Expr } from 'https://deno.land/x/pgsql_ast_parser@7.0.2/mod.ts';
import { ReadOnlyTable } from './readonly-table.ts';
import { buildValue } from '../expression-builder.ts';
import { Types } from '../datatypes/index.ts';

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

    constructor(owner: _ISchema, alias: string, items: Expr[][], columnNames: string[]) {
        super(owner);
        const len = new Set(items.map(x => x.length));
        if (len.size !== 1) {
            throw new QueryError('VALUES lists must all be the same length');
        }
        if (columnNames?.length > items[0].length) {
            throw new QueryError(`table "${alias}" has ${items[0].length} columns available but ${columnNames.length} columns specified`);
        }
        let builtVals = items.map(vals => vals.map(e => buildValue(owner.dualTable.selection, e)));
        const types = items[0].map((_, i) => preferedType(builtVals.map(x => x[i].type)));
        this._schema = {
            name: alias,
            fields: types.map<SchemaField>((type, i) => {
                return {
                    type,
                    name: columnNames?.[i] ?? `column${i}`,
                }
            })
        };
        this.items = builtVals.map(vals => {
            const ret = { [this.symbol]: true } as any;
            setId(ret, 'vtbl' + (++cnt));
            for (let i = 0; i < vals.length; i++) {
                ret[this._schema.fields[i].name] = vals[i].convert(types[i]).get();
            }
            return ret;
        });
    }
}

function preferedType(types: _IType[]) {
    return types.reduce((a, b) => {
        const ret = a.prefer(b);
        if (!ret) {
            throw new QueryError('Incompatible value types');
        }
        return ret;
    }, types[0]);
}