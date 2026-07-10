import { _ISchema, _Transaction, _IStatementExecutor, _IStatement, IValue, _IType, StatementResult, Parameter } from '../../interfaces-private.ts';
import { CreateDomainStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { withParameters } from '../../parser/context.ts';
import { Evaluator } from '../../evaluator.ts';
import { DomainType, DomainCheck } from '../../datatypes/t-domain.ts';
import { ignore } from '../../utils.ts';

const DUMMY_ROW = {};

export class CreateDomain extends ExecHelper implements _IStatementExecutor {
    private onSchema: _ISchema;
    private name: string;
    private base: _IType;
    private notNull: boolean;
    private checks: DomainCheck[];

    constructor({ schema }: _IStatement, p: CreateDomainStatement) {
        super(p);
        this.onSchema = schema.getThisOrSiblingFor(p.name);
        this.name = p.name.name;
        this.base = schema.getType(p.dataType);
        ignore(p.collate);
        // DEFAULT on a domain is parsed but not applied as a column default yet
        ignore(p.default);

        const constraints = p.constraints ?? [];
        this.notNull = constraints.some(c => c.type === 'not null');

        // Compile each CHECK: `value` is bound to a placeholder whose current candidate is
        // set right before evaluation (single-threaded, so the closure is safe).
        this.checks = [];
        let checkIdx = 0;
        for (const c of constraints) {
            if (c.type !== 'check') {
                continue;
            }
            const holder: { current: any } = { current: undefined };
            // `value` inside a domain CHECK resolves to this placeholder (id === 'value',
            // matched by name via the parameters stack)
            const placeholder = new Evaluator(
                this.base, 'value', `domain-value-${this.name}-${checkIdx}`, [],
                () => holder.current, { forceNotConstant: true });
            const param: Parameter = { index: 0, value: placeholder };
            const expr = c.expr;
            const val: IValue = withParameters([param], () => buildValue(expr));
            const cname = c.constraintName?.name ?? `${this.name}_check${checkIdx > 0 ? checkIdx : ''}`;
            this.checks.push({
                name: cname,
                run: (candidate, t) => {
                    holder.current = candidate;
                    // `value` comes from the placeholder closure, not the row, but get()
                    // guards against a nullish row - pass a dummy truthy one
                    return val.get(DUMMY_ROW, t);
                },
            });
            checkIdx++;
        }
    }

    execute(t: _Transaction): StatementResult {
        t = t.fullCommit();
        new DomainType(this.onSchema, this.name, this.base, this.notNull, this.checks).install();
        t = t.fork();
        return this.noData(t, 'CREATE DOMAIN');
    }
}
