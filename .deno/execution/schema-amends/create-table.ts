import { _ISchema, _Transaction, SchemaField, NotSupported, _ITable, _IStatementExecutor, Schema, DataType, QueryError, asTable } from '../../interfaces-private.ts';
import { CreateTableStatement, QName } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ignore, Optional } from '../../utils.ts';
import { checkExistence, ExecHelper } from '../exec-utils.ts';
import { buildCtx } from '../../parser/context.ts';
import { setupPartitionedParent, setupChildPartition } from '../partitioning.ts';

export class ExecuteCreateTable extends ExecHelper implements _IStatementExecutor {
    private toDeclare: Schema;
    private ifNotExists: boolean;
    private name: QName;
    private schema: _ISchema;
    private p: CreateTableStatement;

    constructor(p: CreateTableStatement) {
        super(p);
        this.p = p;
        const { db } = buildCtx();
        this.schema = db.getSchema(p.name.schema);
        let fields: SchemaField[] = [];
        for (const f of p.columns) {
            switch (f.kind) {
                case 'column':
                    if (!f.dataType.kind && f.dataType.name === DataType.record) {
                        throw new QueryError(`column "${f.name.name}" has pseudo-type record`, '42P16');
                    }
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
        // tablespaces are physical storage; meaningless in-memory
        ignore(p.tablespace);
        // partition clauses are consumed in execute() (against the raw statement); mark
        // their sub-AST read so the coverage checker is satisfied at plan time.
        ignore(p.partitionBy, p.partitionOf);
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

        const partitionOf = this.p.partitionOf;
        const partitionBy = this.p.partitionBy;

        // perform creation
        checkExistence(this.schema, this.name, this.ifNotExists, () => {
            if (partitionOf) {
                // a partition inherits its parent's columns
                const parent = asTable(this.schema.getObject(partitionOf.parent));
                const fields: SchemaField[] = parent.selection.columns.map(c => ({
                    name: c.id!,
                    type: c.type,
                }));
                const child = this.schema.declareTable({ name: this.toDeclare.name, fields });
                setupChildPartition(parent, child, partitionOf.bound);
            } else {
                const table = this.schema.declareTable(this.toDeclare);
                if (partitionBy) {
                    setupPartitionedParent(table, partitionBy);
                }
            }
        });


        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}