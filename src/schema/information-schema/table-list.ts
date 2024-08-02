import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _ISchema, _Transaction, _Explainer } from '../../interfaces-private';
import { Schema, nil } from '../../interfaces';
import { Types } from '../../datatypes';
import { TableIndex } from '../table-index';
import { ReadOnlyTable } from '../readonly-table';

const IS_SCHEMA = Symbol('_is_schema');
export class TablesSchema extends ReadOnlyTable implements _ITable {

    get ownSymbol() {
        return IS_SCHEMA;
    }

    isOriginOf(v: IValue): boolean {
        return v.origin === this || v.origin === this.selection;
    }

    _schema: Schema = {
        name: 'tables',
        fields: [
            { name: 'table_catalog', type: Types.text() }
            , { name: 'table_schema', type: Types.text() }
            , { name: 'table_name', type: Types.text() }
            , { name: 'table_type', type: Types.text() }
            , { name: 'self_referencing_column_name', type: Types.text() }
            , { name: 'reference_generation', type: Types.text() }
            , { name: 'user_defined_type_catalog', type: Types.text() }
            , { name: 'user_defined_type_schema', type: Types.text() }
            , { name: 'user_defined_type_name', type: Types.text() }
            , { name: 'is_insertable_into', type: Types.text(3) }
            , { name: 'is_typed', type: Types.text(3) }
            , { name: 'commit_action', type: Types.text() }
        ]
    };

    entropy(t: _Transaction): number {
        return this.db.listSchemas()
            .reduce((tot, s) => tot + s.tablesCount(t), 0);
    }

    *enumerate(t: _Transaction) {
        for (const s of this.db.listSchemas()) {
            for (const it of s.listTables(t)) {
                yield this.make(it);
            }
        }
    }

    make(t: _ITable): any {
        if (!t) {
            return null;
        }
        const ret = {
            table_catalog: 'pgmem',
            table_schema: 'public',
            table_name: t.name,
            table_type: 'BASE TABLE',
            self_referencing_column_name: null,
            reference_generation: null,
            user_defined_type_catalog: null,
            user_defined_type_schema: null,
            user_defined_type_name: null,
            is_insertable_into: 'YES',
            is_typed: 'NO',
            commit_action: null,
            [IS_SCHEMA]: true,
        };
        setId(ret, '/schema/table/' + t.name);
        return ret;
    }

    hasItem(value: any): boolean {
        return !!value?.[IS_SCHEMA];
    }

    getIndex(forValue: IValue): _IIndex | nil {
        if (forValue?.id === 'table_name') {
            return new TableIndex(this, forValue);
        }
        return null;
    }

}
