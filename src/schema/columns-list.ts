import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _Transaction, _ISchema } from '../interfaces-private';
import { Selection } from '../transforms/selection';
import { ReadOnlyError, NotSupported, Schema, nil } from '../interfaces';
import { Types } from '../datatypes';
import { TableIndex } from './table-index';
import { ReadOnlyTable } from './readonly-table';

const IS_SCHEMA = Symbol('_is_colmun');
export class ColumnsListSchema extends ReadOnlyTable implements _ITable {

    get ownSymbol() {
        return IS_SCHEMA;
    }

    _schema: Schema = {
        name: 'columns',
        fields: [
            { name: 'table_catalog', type: Types.text() }
            , { name: 'table_schema', type: Types.text() }
            , { name: 'table_name', type: Types.text() }
            , { name: 'column_name', type: Types.text() }
            , { name: 'ordinal_position', type: Types.int }
            , { name: 'column_default', type: Types.text() }
            , { name: 'is_nullable', type: Types.text(3) }
            , { name: 'data_type', type: Types.text() }
            , { name: 'character_maximum_length', type: Types.int }
            , { name: 'character_octet_length', type: Types.int }
            , { name: 'numeric_precision', type: Types.int }
            , { name: 'numeric_precision_radix', type: Types.int }
            , { name: 'numeric_scale', type: Types.int }
            , { name: 'datetime_precision', type: Types.int }
            , { name: 'interval_type', type: Types.text() }
            , { name: 'interval_precision', type: Types.int }
            , { name: 'character_set_catalog', type: Types.text() }
            , { name: 'character_set_schema', type: Types.text() }
            , { name: 'character_set_name', type: Types.text() }
            , { name: 'collation_catalog', type: Types.text() }
            , { name: 'collation_schema', type: Types.text() }
            , { name: 'collation_name', type: Types.text() }
            , { name: 'domain_catalog', type: Types.text() }
            , { name: 'domain_schema', type: Types.text() }
            , { name: 'domain_name', type: Types.text() }
            , { name: 'udt_catalog', type: Types.text() } // <====
            , { name: 'udt_schema', type: Types.text() } // <====
            , { name: 'udt_name', type: Types.text() } // <====
            , { name: 'scope_catalog', type: Types.text() } // <====
            , { name: 'scope_schema', type: Types.text() } // <====
            , { name: 'scope_name', type: Types.text() } // <====
            , { name: 'maximum_cardinality', type: Types.int } // <====
            , { name: 'dtd_identifier', type: Types.int } // <=== INDEX
            , { name: 'is_self_referencing', type: Types.text(3) }
            , { name: 'is_identity', type: Types.text(3) } // <==
            , { name: 'identity_generation', type: Types.text() } // <==
            , { name: 'identity_start', type: Types.text() } // <==
            , { name: 'identity_document', type: Types.text() } // <==
            , { name: 'identity_increment', type: Types.text() } // <==
            , { name: 'identity_maximum', type: Types.text() } // <==
            , { name: 'identity_minimum', type: Types.text() } // <==
            , { name: 'identity_cycle', type: Types.text(3) } // <==
            , { name: 'is_generated', type: Types.text() } // <==
            , { name: 'generation_expression', type: Types.text() } // <==
            , { name: 'is_updatable', type: Types.text(3) } // <==
        ]
    };


    entropy(t: _Transaction): number {
        return this.schema.db.listSchemas()
            .reduce((tot, s) => tot + s.tablesCount(t) * 10, 0);
    }

    *enumerate(t: _Transaction) {
        for (const s of this.schema.db.listSchemas()) {
            for (const it of s.listTables(t)) {
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
            table_catalog: 'pgmem',
            table_schema: 'public',
            table_name: table.name,
            column_name: t.id,
            ordinal_position: i,
            is_nullable: 'NO',
            data_type: t.type.primary, // <== todo
            numeric_precision: null, // <== todo
            numeric_precision_radix: null, // <== todo
            numeric_scale: null, // <== todo

            udt_catalog: 'pgmem',
            udt_schema: 'pg_catalog',
            udt_name: t.type.primary, // <== todo

            dtd_identifier: i, // <== todo

            is_self_referencing: 'NO',
            is_identity: 'NO',

            is_updatable: 'YES',
            is_generated: 'NEVER',
            identity_cycle: 'NO',


            [IS_SCHEMA]: true,
        };
        setId(ret, `/schema/${table.schema.name}/table/${table.name}/${i}`);
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

    *itemsByTable(table: string | _ITable, t: _Transaction): IterableIterator<any> {
        if (typeof table === 'string') {
            for (const s of this.schema.db.listSchemas()) {
                const got = s.getTable(table, true);
                if (got) {
                    yield* this.itemsByTable(got, t);
                }
            }
        } else {
            let i = 0;
            for (const f of table.selection.columns) {
                yield this.make(table, ++i, f);
            }
        }
    }

}
