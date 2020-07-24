import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _Transaction, _IQuery } from '../interfaces-private';
import { Selection } from '../transforms/selection';
import { ReadOnlyError, NotSupported, Schema } from '../interfaces';
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
            { id: 'table_catalog', type: Types.text() }
            , { id: 'table_schema', type: Types.text() }
            , { id: 'table_name', type: Types.text() }
            , { id: 'column_name', type: Types.text() }
            , { id: 'ordinal_position', type: Types.int }
            , { id: 'column_default', type: Types.text() }
            , { id: 'is_nullable', type: Types.text(3) }
            , { id: 'data_type', type: Types.text() }
            , { id: 'character_maximum_length', type: Types.int }
            , { id: 'character_octet_length', type: Types.int }
            , { id: 'numeric_precision', type: Types.int }
            , { id: 'numeric_precision_radix', type: Types.int }
            , { id: 'numeric_scale', type: Types.int }
            , { id: 'datetime_precision', type: Types.int }
            , { id: 'interval_type', type: Types.text() }
            , { id: 'interval_precision', type: Types.int }
            , { id: 'character_set_catalog', type: Types.text() }
            , { id: 'character_set_schema', type: Types.text() }
            , { id: 'character_set_name', type: Types.text() }
            , { id: 'collation_catalog', type: Types.text() }
            , { id: 'collation_schema', type: Types.text() }
            , { id: 'collation_name', type: Types.text() }
            , { id: 'domain_catalog', type: Types.text() }
            , { id: 'domain_schema', type: Types.text() }
            , { id: 'domain_name', type: Types.text() }
            , { id: 'udt_catalog', type: Types.text() } // <====
            , { id: 'udt_schema', type: Types.text() } // <====
            , { id: 'udt_name', type: Types.text() } // <====
            , { id: 'scope_catalog', type: Types.text() } // <====
            , { id: 'scope_schema', type: Types.text() } // <====
            , { id: 'scope_name', type: Types.text() } // <====
            , { id: 'maximum_cardinality', type: Types.int } // <====
            , { id: 'dtd_identifier', type: Types.int } // <=== INDEX
            , { id: 'is_self_referencing', type: Types.text(3) }
            , { id: 'is_identity', type: Types.text(3) } // <==
            , { id: 'identity_generation', type: Types.text() } // <==
            , { id: 'identity_start', type: Types.text() } // <==
            , { id: 'identity_document', type: Types.text() } // <==
            , { id: 'identity_increment', type: Types.text() } // <==
            , { id: 'identity_maximum', type: Types.text() } // <==
            , { id: 'identity_minimum', type: Types.text() } // <==
            , { id: 'identity_cycle', type: Types.text(3) } // <==
            , { id: 'is_generated', type: Types.text() } // <==
            , { id: 'generation_expression', type: Types.text() } // <==
            , { id: 'is_updatable', type: Types.text(3) } // <==
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
        for (const { id } of this._schema.fields) {
            ret[id] = null;
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

    getIndex(forValue: IValue<any>): _IIndex<any> {
        if (forValue.id === 'table_name') {
            return new TableIndex(this, forValue);
        }
        return null;
    }

    *itemsByTable(table: string | _ITable, t: _Transaction) {
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
