import { _Transaction, asTable, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor } from '../../interfaces-private.ts';
import { CreateIndexStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ignore } from '../../utils.ts';
import { ExecHelper } from '../exec-utils.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { withSelection } from '../../parser/context.ts';

export class CreateIndexExec extends ExecHelper implements _IStatementExecutor {
    private onTable: _ITable;
    private indexDef: CreateIndexDef;

    constructor({ schema }: _IStatement, p: CreateIndexStatement) {
        super(p);
        const indexName = p.indexName?.name;
        this.onTable = asTable(schema.getObject(p.table));
        // check that index algorithm is supported
        if (p.using && p.using.name.toLowerCase() !== 'btree') {
            if (schema.db.options.noIgnoreUnsupportedIndices) {
                throw new NotSupported('index type: ' + p.using);
            }
            ignore(p);
        }

        this.indexDef = withSelection(this.onTable.selection, () => {

            // index columns
            const columns = p.expressions
                .map<CreateIndexColDef>(x => {
                    return {
                        value: buildValue(x.expression),
                        nullsLast: x.nulls === 'last', // nulls are first by default
                        desc: x.order === 'desc',
                    }
                });

            // compile predicate (if any)
            const predicate = p.where && buildValue(p.where);

            return {
                columns,
                indexName,
                unique: p.unique,
                ifNotExists: p.ifNotExists,
                predicate,
            };
        });
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index creation does not support further rollbacks)
        t = t.fullCommit();

        // create index
        this.onTable
            .createIndex(t, this.indexDef);

        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}
