import { _Transaction, asTable, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor } from '../../interfaces-private';
import { CreateIndexStatement } from 'pgsql-ast-parser';
import { ignore } from 'utils';
import { resultNoData } from '../exec-utils';
import { buildValue } from '../../parser/expression-builder';

export class CreateIndexExec implements _IStatementExecutor {
    private onTable: _ITable;
    private indexDef: CreateIndexDef;

    constructor({ schema }: _IStatement, private p: CreateIndexStatement) {

        // check that index algorithm is supported
        const indexName = p.indexName?.name;
        this.onTable = asTable(schema.getObject(p.table));
        if (p.using && p.using.name.toLowerCase() !== 'btree') {
            if (schema.db.options.noIgnoreUnsupportedIndices) {
                throw new NotSupported('index type: ' + p.using);
            }
            ignore(p);
        }

        // index columns
        const columns = p.expressions
            .map<CreateIndexColDef>(x => {
                return {
                    value: buildValue(this.onTable.selection, x.expression),
                    nullsLast: x.nulls === 'last', // nulls are first by default
                    desc: x.order === 'desc',
                }
            });

        // compile predicate (if any)
        const predicate = p.where && buildValue(this.onTable.selection, p.where);

        this.indexDef = {
            columns,
            indexName,
            unique: p.unique,
            ifNotExists: p.ifNotExists,
            predicate,
        };
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
        return resultNoData('CREATE', this.p, t);
    }
}
