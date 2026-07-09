import { _ISchema, _Transaction, _ITable, _IStatementExecutor, asTable, StatementResult, _IStatement, QueryError } from '../../interfaces-private';
import { CreateTriggerStatement } from 'pgsql-ast-parser';
import { ignore } from '../../utils';
import { ExecHelper } from '../exec-utils';
import { TriggerOp } from '../triggers';
import { compileTriggerWhen, TriggerContext } from '../plpgsql';

export class CreateTrigger extends ExecHelper implements _IStatementExecutor {
    private table: _ITable;
    private functionSchema: _ISchema;
    private events: TriggerOp[];
    private when?: (ctx: TriggerContext, t: _Transaction) => any;
    private updateColumns?: string[];

    constructor({ schema }: _IStatement, private p: CreateTriggerStatement) {
        super(p);
        this.table = asTable(schema.getObject(p.table));
        this.functionSchema = schema.getThisOrSiblingFor(p.execute.function);
        this.events = p.events
            .filter(e => e.event !== 'truncate')
            .map(e => e.event as TriggerOp);
        // `UPDATE OF a, b` -> the column ids to watch for changes
        const updateEvent = p.events.find(e => e.event === 'update');
        if (updateEvent?.columns?.length) {
            this.updateColumns = updateEvent.columns.map(c => this.table.getColumnRef(c.name).expression.id!);
        }
        // WHEN condition, compiled with NEW/OLD in scope
        if (p.when) {
            this.when = compileTriggerWhen(this.table, p.when);
        }
        // v1 scope: trigger arguments (TG_ARGV) and the CONSTRAINT flag are parsed but not
        // enforced (TG_ARGV needs the fuller plpgsql variable machinery)
        p.execute.arguments.forEach(ignore);
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
            when: this.when,
            updateColumns: this.updateColumns,
        });
        t = t.fork();
        return this.noData(t, 'CREATE TRIGGER');
    }
}
