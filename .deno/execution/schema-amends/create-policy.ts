import { _ISchema, _Transaction, _ITable, _IStatementExecutor, asTable, StatementResult, _IStatement } from '../../interfaces-private.ts';
import { CreatePolicyStatement, DropPolicyStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ignore } from '../../utils.ts';
import { ExecHelper } from '../exec-utils.ts';

export class CreatePolicy extends ExecHelper implements _IStatementExecutor {
    private table: _ITable;

    constructor({ schema }: _IStatement, private p: CreatePolicyStatement) {
        super(p);
        this.table = asTable(schema.getObject(p.table));
        // the predicates are stored raw and compiled lazily at enforcement time
        ignore(p.using);
        ignore(p.withCheck);
        p.roles?.forEach(ignore);
    }

    execute(t: _Transaction): StatementResult {
        this.table.createPolicy({
            name: this.p.name.name,
            // postgres default is PERMISSIVE
            permissive: this.p.permissive ?? true,
            command: this.p.for ?? 'all',
            roles: (this.p.roles ?? []).map(r => r.name),
            using: this.p.using ?? null,
            withCheck: this.p.withCheck ?? null,
        });
        return this.noData(t, 'CREATE POLICY');
    }
}

export class DropPolicy extends ExecHelper implements _IStatementExecutor {
    private table: _ITable;

    constructor({ schema }: _IStatement, private p: DropPolicyStatement) {
        super(p);
        this.table = asTable(schema.getObject(p.table));
        ignore(p.ifExists);
    }

    execute(t: _Transaction): StatementResult {
        this.table.dropPolicy(this.p.name.name, !!this.p.ifExists);
        return this.noData(t, 'DROP POLICY');
    }
}
