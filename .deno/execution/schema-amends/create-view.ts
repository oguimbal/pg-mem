import { _Transaction, asTable, _ISchema, NotSupported, CreateIndexColDef, _ITable, CreateIndexDef, _IStatement, _IStatementExecutor, asView, _IView, QueryError } from '../../interfaces-private.ts';
import { CreateViewStatement, SelectedColumn } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { ignore } from '../../utils.ts';
import { View } from '../../schema/view.ts';
import { buildSelect } from '../select.ts';

export class CreateView extends ExecHelper implements _IStatementExecutor {
    private schema: _ISchema;
    private drop: boolean;
    existing: _IView | null;
    toRegister: View;


    constructor(st: _IStatement, p: CreateViewStatement) {
        super(p);
        this.schema = st.schema.getThisOrSiblingFor(p.name);
        // check existence
        this.existing = asView(this.schema.getObject(p.name, { nullIfNotFound: true }));
        ignore(p.orReplace);
        this.drop = !!(p.orReplace && this.existing);

        let view = buildSelect(p.query);

        // optional column mapping
        if (p.columnNames?.length) {
            if (p.columnNames.length > view.columns.length) {
                throw new QueryError('CREATE VIEW specifies more column names than columns', '42601');
            }
            view = view.select(view.columns.map<string | SelectedColumn>((x, i) => {
                const alias = p.columnNames?.[i]?.name;
                if (!alias) {
                    return x.id!;
                }
                return {
                    expr: { type: 'ref', name: x.id! },
                    alias: { name: alias },
                }
            }));
        }

        this.toRegister = new View(this.schema, p.name.name, view);
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        // drop if needed
        if (this.existing && this.drop) {
            this.existing.drop(t);
        }

        // view creation
        this.toRegister.register();

        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}
