import { _ITable, _Transaction, _ISelection, _IStatement, _IStatementExecutor, StatementResult, asTable, QueryError, nil, IValue, Row, getId } from '../../interfaces-private.ts';
import { MergeStatement, MergeAction, Expr } from 'https://deno.land/x/pgsql_ast_parser@12.0.2/mod.ts';
import { buildSelect } from '../select.ts';
import { Selection } from '../../transforms/selection.ts';
import { JoinSelection } from '../../transforms/join.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { withSelection, buildCtx } from '../../parser/context.ts';
import { createSetter, Setter } from './mutation-base.ts';
import { checkWriteRls } from '../rls-enforce.ts';
import { deepCloneSimple } from '../../utils.ts';
import { ExecHelper, locOf } from '../exec-utils.ts';

interface CompiledAction {
    action: MergeAction;
    and: IValue | nil;
    setter?: Setter;
    insertColumns?: string[];
    insertValues?: IValue[];
}

export class MergeExec extends ExecHelper implements _IStatementExecutor {
    private table: _ITable;
    private join: JoinSelection;
    private on: IValue;
    private matched: CompiledAction[] = [];
    private notMatched: CompiledAction[] = [];
    private p: MergeStatement;

    constructor(_st: _IStatement, p: MergeStatement) {
        super(p);
        this.p = p;
        const { schema } = buildCtx();
        this.table = asTable(schema.getObject(p.target));

        // build the source × target join so ON / AND / SET / INSERT expressions can
        // resolve refs on both sides (source alias + target alias). A trivial ON TRUE is
        // used: the real ON condition is evaluated per candidate pair below.
        let sel: _ISelection = buildSelect({
            type: 'select',
            from: [
                p.source,
                {
                    type: 'table',
                    name: p.target,
                    join: { type: 'INNER JOIN', on: { type: 'boolean', value: true } },
                },
            ],
            columns: [{ expr: { type: 'ref', name: '*' } }],
        });
        // a bare `SELECT *` over a join is optimized to the JoinSelection itself;
        // otherwise it is wrapped in a Selection whose base is the join.
        if (sel instanceof JoinSelection) {
            this.join = sel;
        } else if (sel instanceof Selection && sel.base instanceof JoinSelection) {
            this.join = sel.base;
        } else {
            throw new QueryError('Invalid MERGE source');
        }

        this.on = withSelection(this.join, () => buildValue(p.on));

        for (const a of p.actions) {
            const compiled: CompiledAction = {
                action: a,
                and: a.and ? withSelection(this.join, () => buildValue(a.and as Expr)) : null,
            };
            const then = a.then;
            if (then.type === 'update') {
                compiled.setter = createSetter(this.table, this.join, then.sets);
            } else if (then.type === 'insert') {
                const cols = then.columns?.map(c => c.name)
                    ?? this.table.selection.columns.map(c => c.id!);
                if (then.values) {
                    if (then.values.length > cols.length) {
                        throw new QueryError('MERGE INSERT has more expressions than target columns');
                    }
                    compiled.insertColumns = cols.slice(0, then.values.length);
                    compiled.insertValues = withSelection(this.join, () => then.values!.map((e, i) => {
                        const col = this.table.selection.getColumn(compiled.insertColumns![i]);
                        return buildValue(e).cast(col.type);
                    }));
                } else {
                    // DEFAULT VALUES
                    compiled.insertColumns = [];
                    compiled.insertValues = [];
                }
            }
            if (a.when === 'matched') {
                this.matched.push(compiled);
            } else {
                this.notMatched.push(compiled);
            }
        }
    }

    execute(t: _Transaction): StatementResult {
        // snapshot the target rows up-front so mutations during the loop don't invalidate
        // iteration, and new (not-matched) inserts are never re-matched.
        const targetRows: Row[] = [...this.table.selection.enumerate(t)];
        // each target row may be affected at most once (Postgres semantics)
        const affectedIds = new Set<string>();
        let affected = 0;

        for (const src of this.join.restrictive.enumerate(t)) {
            const matches = targetRows.filter(tr => {
                if (affectedIds.has(getId(tr))) { return false; }
                return this.on.get(this.join.buildItem(src, tr), t) === true;
            });

            if (matches.length) {
                const combined = this.join.buildItem(src, matches[0]);
                const chosen = this.matched.find(c => !c.and || c.and.get(combined, t) === true);
                if (!chosen || chosen.action.then.type === 'do nothing') { continue; }
                for (const tr of matches) {
                    const pair = this.join.buildItem(src, tr);
                    if (chosen.action.then.type === 'delete') {
                        this.table.delete(t, tr);
                    } else {
                        const data = deepCloneSimple(tr);
                        chosen.setter!(t, data, pair);
                        checkWriteRls(this.table, 'update', data, t);
                        this.table.update(t, data);
                    }
                    affectedIds.add(getId(tr));
                    affected++;
                }
            } else {
                const combined = this.join.buildItem(src, null as any);
                const chosen = this.notMatched.find(c => !c.and || c.and.get(combined, t) === true);
                if (!chosen || chosen.action.then.type !== 'insert') { continue; }
                const row: any = {};
                chosen.insertValues!.forEach((v, i) => {
                    row[chosen.insertColumns![i]] = v.get(combined, t);
                });
                this.table.fillDefaults(row, t);
                checkWriteRls(this.table, 'insert', row, t);
                this.table.doInsert(t, row);
                affected++;
            }
        }

        return {
            result: {
                command: 'MERGE',
                fields: [],
                rowCount: affected,
                rows: [],
                location: locOf(this.p),
            },
            state: t,
        };
    }
}
