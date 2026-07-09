import { _IView, _ISelection, _Transaction, _IStatementExecutor, StatementResult, IValue, QueryError, nil } from '../../interfaces-private';
import { InsertStatement, UpdateStatement, DeleteStatement } from 'pgsql-ast-parser';
import { ExecHelper, locOf } from '../exec-utils';
import { buildValue } from '../../parser/expression-builder';
import { withSelection } from '../../parser/context';
import { buildValues, buildSelect } from '../select';
import { fireInsteadOf } from '../triggers';

/**
 * INSERT / UPDATE / DELETE on a view carrying an INSTEAD OF trigger: no rows are mutated;
 * instead the trigger fires once per row (the trigger body does the real work).
 */
export class InsteadOfView extends ExecHelper implements _IStatementExecutor {
    private run: (t: _Transaction) => number;
    private command: string;

    constructor(
        private view: _IView,
        private ast: InsertStatement | UpdateStatement | DeleteStatement,
    ) {
        super(ast);
        this.command = ast.type.toUpperCase();
        // materialise a row into a plain object keyed by the view's column ids
        const materialize = (raw: any, t: _Transaction) => {
            const out: any = {};
            for (const c of view.selection.columns) {
                out[c.id!] = c.get(raw, t);
            }
            return out;
        };

        if (ast.type === 'insert') {
            const source: _ISelection = ast.insert.type === 'values'
                ? buildValues(ast.insert, true)
                : buildSelect(ast.insert);
            const insertCols = ast.columns?.map(x => x.name)
                ?? view.selection.columns.map(c => c.id!).slice(0, source.columns.length);
            const colIds = insertCols.map(c => view.selection.getColumn(c).id!);
            this.run = (t) => {
                let count = 0;
                for (const o of source.enumerate(t)) {
                    const neu: any = {};
                    colIds.forEach((cid, i) => neu[cid] = source.columns[i].get(o, t));
                    fireInsteadOf(view, 'insert', neu, null, t);
                    count++;
                }
                return count;
            };
        } else if (ast.type === 'delete') {
            const mutated = view.selection.filter(ast.where);
            this.run = (t) => {
                let count = 0;
                for (const raw of mutated.enumerate(t)) {
                    fireInsteadOf(view, 'delete', null, materialize(raw, t), t);
                    count++;
                }
                return count;
            };
        } else {
            // update: OLD is the current row; NEW is OLD with the SET assignments applied
            const mutated = view.selection.filter(ast.where);
            const sets = withSelection(view.selection, () => ast.sets.map(s => ({
                colId: view.selection.getColumn(s.column.name).id!,
                value: s.value.type === 'default' ? null : buildValue(s.value),
            })));
            this.run = (t) => {
                let count = 0;
                for (const raw of mutated.enumerate(t)) {
                    const old = materialize(raw, t);
                    const neu = { ...old };
                    for (const s of sets) {
                        if (s.value) { neu[s.colId] = (s.value as IValue).get(raw, t); }
                    }
                    fireInsteadOf(view, 'update', neu, old, t);
                    count++;
                }
                return count;
            };
        }
    }

    execute(t: _Transaction): StatementResult {
        const count = this.run(t);
        return {
            result: {
                command: this.command,
                fields: [],
                rowCount: count,
                rows: [],
                location: locOf(this.ast),
            },
            state: t,
        };
    }
}
