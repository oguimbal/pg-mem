import { _ISchema, _Transaction, _ITable, _IStatementExecutor, asTable, StatementResult, _IStatement, QueryError } from '../../interfaces-private';
import { CreateTriggerStatement } from 'pgsql-ast-parser';
import { ignore } from '../../utils';
import { ExecHelper } from '../exec-utils';
import { TriggerOp } from '../triggers';

export class CreateTrigger extends ExecHelper implements _IStatementExecutor {
    private table: _ITable;
    private functionSchema: _ISchema;
    private events: TriggerOp[];

    constructor({ schema }: _IStatement, private p: CreateTriggerStatement) {
        super(p);
        this.table = asTable(schema.getObject(p.table));
        this.functionSchema = schema.getThisOrSiblingFor(p.execute.function);
        this.events = p.events
            .filter(e => e.event !== 'truncate')
            .map(e => e.event as TriggerOp);
        // v1 scope: WHEN condition, trigger arguments, per-column UPDATE OF, and the
        // CONSTRAINT flag are parsed but not enforced
        ignore(p.when);
        p.execute.arguments.forEach(ignore);
        p.events.forEach(e => e.columns?.forEach(ignore));
        if (p.timing === 'instead of') {
            throw new QueryError('INSTEAD OF triggers are only supported on views', '42809');
        }
    }

    execute(t: _Transaction): StatementResult {
        t = t.fullCommit();
        this.table.createTrigger({
            name: this.p.name.name,
            timing: this.p.timing,
            events: this.events,
            forEach: this.p.forEach,
            functionName: this.p.execute.function.name,
            functionSchema: this.functionSchema,
        });
        t = t.fork();
        return this.noData(t, 'CREATE TRIGGER');
    }
}
