import { _ITable, _ISelection, IValue, _IIndex, _IDb, IndexKey, setId, _IQuery, _Transaction } from '../interfaces-private';
import { Selection } from '../transforms/selection';
import { ReadOnlyError, NotSupported } from '../interfaces';
import { Types } from '../datatypes';
import { TableIndex } from './table-index';

const IS_SCHEMA = Symbol('_is_schema');
export class TablesSchema implements _ITable {

    hidden = true;

    get ownSymbol() {
        return IS_SCHEMA;
    }

    get name() {
        return 'tables';
    }

    selection: _ISelection<any> = new Selection(this, {
        schema: {
            name: 'tables',
            fields: [
                { id: 'table_catalog', type: Types.text() }
                , { id: 'table_schema', type: Types.text() }
                , { id: 'table_name', type: Types.text() }
                , { id: 'table_type', type: Types.text() }
                , { id: 'self_referencing_column_name', type: Types.text() }
                , { id: 'reference_generation', type: Types.text() }
                , { id: 'user_defined_type_catalog', type: Types.text() }
                , { id: 'user_defined_type_schema', type: Types.text() }
                , { id: 'user_defined_type_name', type: Types.text() }
                , { id: 'is_insertable_into', type: Types.text(3) }
                , { id: 'is_typed', type: Types.text(3) }
                , { id: 'commit_action', type: Types.text() }
            ]
        }
    });

    constructor(readonly schema: _IQuery) {
    }

    insert(toInsert: any): void {
        throw new ReadOnlyError('information schema');
    }
    createIndex(): this {
        throw new ReadOnlyError('information schema');
    }

    setReadonly(): this {
        throw new ReadOnlyError('information schema');
    }

    entropy(t: _Transaction): number {
        return this.schema.db.listSchemas()
            .reduce((tot, s) => tot + s.tablesCount(t), 0);
    }

    *enumerate(t: _Transaction) {
        for (const s of this.schema.db.listSchemas()) {
            for (const it of s.listTables(t)) {
                yield this.make(it);
            }
        }
    }

    make(t: _ITable<any>): any {
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

    getIndex(forValue: IValue<any>): _IIndex<any> {
        if (forValue.id === 'table_name') {
            return new TableIndex(this, forValue);
        }
        return null;
    }

    on(): void {
        throw new NotSupported('subscribing information schema');
    }

    *itemsByTable(table: string, t: _Transaction) {
        for (const s of this.schema.db.listSchemas()) {
            const got = s.getTable(table, true)
            if (got) {
                yield this.make(got);
            }
        }
    }

}
