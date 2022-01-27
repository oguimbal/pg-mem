import { _ISchema, _Transaction, SchemaField, NotSupported, _ITable } from '../interfaces-private';
import { CreateTableStatement, QName } from 'pgsql-ast-parser';
import { ignore, Optional } from '../utils';
import { checkExistence } from './exec-utils';

export class ExecuteCreateTable {
    schema: _ISchema;
    constructor(inSchema: _ISchema, private statement: CreateTableStatement) {
        const name: QName = statement.name;
        this.schema = inSchema.getThisOrSiblingFor(name);
    }

    execute(t: _Transaction) {
        const p = this.statement;
        const name: QName = p.name;
        let table: _ITable | null = null;
        checkExistence(this.schema, name, p.ifNotExists, () => {
            let fields: SchemaField[] = [];
            for (const f of p.columns) {
                switch (f.kind) {
                    case 'column':
                        // TODO: #collation
                        ignore(f.collate);
                        const nf = {
                            ...f,
                            name: f.name.name,
                            type: this.schema.getType(f.dataType),
                            serial: !f.dataType.kind && (f.dataType.name === 'serial' || f.dataType.name === 'bigserial'),
                        };
                        delete (nf as Optional<typeof nf>).dataType;
                        fields.push(nf);
                        break;
                    case 'like table':
                        throw new NotSupported('"like table" statement');
                    default:
                        throw NotSupported.never(f);
                }
            }

            // perform creation
            table = this.schema.declareTable({
                name: name.name,
                constraints: p.constraints,
                fields,
            });
        });
        return table;
    }
}