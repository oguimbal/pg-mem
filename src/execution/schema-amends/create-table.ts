import { _ISchema, _Transaction, SchemaField, NotSupported, _ITable, _IStatementExecutor, Schema } from '../../interfaces-private';
import { CreateTableStatement, QName } from 'pgsql-ast-parser';
import { ignore, Optional } from '../../utils';
import { checkExistence, ExecHelper } from '../exec-utils';
import { buildCtx } from '../../parser/context';

export class ExecuteCreateTable extends ExecHelper implements _IStatementExecutor {
    private toDeclare: Schema;
    private ifNotExists: boolean;
    private name: QName;
    private schema: _ISchema;

    constructor(p: CreateTableStatement) {
        super(p);
        const { db } = buildCtx();
        this.schema = db.getSchema(p.name.schema);
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
        this.ifNotExists = !!p.ifNotExists;
        this.name = p.name;
        this.toDeclare = {
            name: p.name.name,
            constraints: p.constraints,
            fields,
        };
    }

    execute(t: _Transaction) {

        // commit pending data before making changes
        //  (because the creation does not support further rollbacks)
        t = t.fullCommit();

        // perform creation
        checkExistence(this.schema, this.name, this.ifNotExists, () => {
            this.schema.declareTable(this.toDeclare);
        });


        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}