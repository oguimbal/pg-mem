import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema, _Transaction } from '../../interfaces-private.ts';
import { nil, Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { TableIndex } from '../table-index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';

const IS_SCHEMA = Symbol('_is_pgconstraint');
export class PgConstraintTable extends ReadOnlyTable implements _ITable {

    get ownSymbol() {
        return IS_SCHEMA;
    }

    _schema: Schema = {
        name: 'pg_constraint',
        fields: [
            { name: 'oid', type: Types.integer } // hidden oid column
            , { name: 'conname', type: Types.text() } // <== 'name' type
            , { name: 'connamespace', type: Types.integer } // <== 'oid' type
            , { name: 'contype', type: Types.text(1) } // <== 'char(1)' type
            , { name: 'condeferrable', type: Types.bool }
            , { name: 'condeferred', type: Types.bool }
            , { name: 'convalidated', type: Types.bool }
            , { name: 'conrelid', type: Types.integer } // <== oid
            , { name: 'contypid', type: Types.integer } // <== oid
            , { name: 'conindid', type: Types.integer } // <== oid
            , { name: 'conparentid', type: Types.integer } // <== oid
            , { name: 'confrelid', type: Types.integer } // <== oid
            , { name: 'confupdtype', type: Types.text(1) } // <== 'char(1)' type
            , { name: 'confdeltype', type: Types.text(1) } // <== 'char(1)' type
            , { name: 'confmatchtype', type: Types.text(1) } // <== 'char(1)' type
            , { name: 'conislocal', type: Types.bool }
            , { name: 'coninhcount', type: Types.integer }
            , { name: 'connoinherit', type: Types.bool }
            , { name: 'conkey', type: Types.integer.asArray() }
            , { name: 'confkey', type: Types.integer.asArray() }
            , { name: 'conpfeqop', type: Types.integer.asArray() } // <== oid[]
            , { name: 'conppeqop', type: Types.integer.asArray() } // <== oid[]
            , { name: 'conffeqop', type: Types.integer.asArray() } // <== oid[]
            , { name: 'conexclop', type: Types.integer.asArray() } // <== oid[]
            , { name: 'conbin', type: Types.text() } // <== weird type
            , { name: 'consrc', type: Types.text() }
        ]
    };


    entropy(t: _Transaction): number {
        return this.db.listSchemas()
            .reduce((tot, s) => tot + s.tablesCount(t) * 10 * 3, 0);
    }

    *enumerate(t: _Transaction) {
        for (const schema of this.db.listSchemas()) {
            for (const it of schema.listTables(t)) {
                yield* this.itemsByTable(it, t);
            }
        }
    }

    make(table: _ITable, i: number, t: IValue<any>): any {
        if (!t) {
            return null;
        }
        let ret = {};
        for (const { name } of this._schema.fields) {
            (ret as any)[name] = null;
        }

        ret = {
            ...ret,
            // table_catalog: 'pgmem',

            [IS_SCHEMA]: true,
        };
        setId(ret, `/schema/${table.ownerSchema.name}/pg_constraint/${table.name}/${i}`);
        return ret;
    }

    hasItem(value: any): boolean {
        return !!value?.[IS_SCHEMA];
    }

    getIndex(forValue: IValue<any>): _IIndex<any> | nil {
        if (forValue.id === 'table_name') {
            return new TableIndex(this, forValue);
        }
        return null;
    }

}
