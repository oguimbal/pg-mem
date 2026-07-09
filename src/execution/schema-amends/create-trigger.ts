import { _ISchema, _Transaction, _ITable, _IView, _IStatementExecutor, asTable, StatementResult, _IStatement, QueryError } from '../../interfaces-private';
import { CreateTriggerStatement } from 'pgsql-ast-parser';
import { buildValue } from '../../parser/expression-builder';
import { ExecHelper } from '../exec-utils';
import { TriggerOp } from '../triggers';
import { compileTriggerWhen, TriggerContext } from '../plpgsql';

export class CreateTrigger extends ExecHelper implements _IStatementExecutor {
    private target: _ITable | _IView;
    private functionSchema: _ISchema;
    private events: TriggerOp[];
    private when?: (ctx: TriggerContext, t: _Transaction) => any;
    private updateColumns?: string[];
    private arguments: string[];

    constructor({ schema }: _IStatement, private p: CreateTriggerStatement) {
        super(p);
        const obj = schema.getObject(p.table);
        const isView = obj.type === 'view';
        // INSTEAD OF triggers live on views; BEFORE/AFTER triggers on tables
        if (isView) {
            if (p.timing !== 'instead of') {
                throw new QueryError(`"${obj.name}" is a view; only INSTEAD OF triggers are supported on views`, '42809');
            }
            this.target = obj as _IView;
        } else {
            this.target = asTable(obj);
            if (p.timing === 'instead of') {
                throw new QueryError('INSTEAD OF triggers are only supported on views', '42809');
            }
        }
        this.functionSchema = schema.getThisOrSiblingFor(p.execute.function);
        this.events = p.events
            .filter(e => e.event !== 'truncate')
            .map(e => e.event as TriggerOp);
        // `UPDATE OF a, b` -> the column ids to watch for changes (tables only)
        const updateEvent = p.events.find(e => e.event === 'update');
        if (!isView && updateEvent?.columns?.length) {
            this.updateColumns = updateEvent.columns.map(c => (this.target as _ITable).getColumnRef(c.name).expression.id!);
        }
        // WHEN condition, compiled with NEW/OLD in scope (tables only for now)
        if (p.when && !isView) {
            this.when = compileTriggerWhen(this.target as _ITable, p.when);
        }
        // literal arguments -> TG_ARGV (they are constant string literals)
        this.arguments = p.execute.arguments.map(a => {
            const v = buildValue(a).get();
            return v == null ? '' : String(v);
        });
    }

    execute(t: _Transaction): StatementResult {
        t = t.fullCommit();
        this.target.createTrigger({
            name: this.p.name.name,
            timing: this.p.timing,
            events: this.events,
            forEach: this.p.forEach,
            functionName: this.p.execute.function.name,
            functionSchema: this.functionSchema,
            when: this.when,
            updateColumns: this.updateColumns,
            arguments: this.arguments,
        });
        t = t.fork();
        return this.noData(t, 'CREATE TRIGGER');
    }
}
