import { _IDb, _ISchema, _Transaction } from '../../interfaces-private';
import { parseSql } from '../../parser/parse-cache';
import { QueryError, NotSupported, DataType } from '../../interfaces';
import { Statement } from 'pgsql-ast-parser';
import { executionCtx as executionCtx } from '../../utils';
import { StatementExec } from '../../execution/statement-exec';
import { buildSelect } from '../../execution/select';

export function registerSqlFunctionLanguage(db: _IDb) {
    db.registerLanguage('sql', ({ code, schema, args, returns }) => {
        // parse SQL
        const _parsed = parseSql(code);
        let parsed: Statement;
        if (Array.isArray(_parsed)) {
            if (_parsed.length !== 1) {
                throw new QueryError(`Expected 1 statement in function, got ${_parsed.length}`);
            }
            parsed = _parsed[0];
        } else {
            parsed = _parsed;
        }
        switch (parsed.type) {
            case 'select':
            case 'union':
            case 'union all':
            case 'with':
            case 'with recursive':
            case 'values':
                break;
            default:
                throw new NotSupported(`Unsupported statement type in function: ${parsed.type}`);
        }

        // visit & compile tree
        const selection = buildSelect(parsed);

        // todo: prepare statement here, to avoid optimization on each call.


        // get the result transformer, based on the expected function output type
        let transformResult: (values: any[], t: _Transaction) => any;
        if (!returns || returns.primary === DataType.null) {
            transformResult = () => null;
        } else if (returns.primary === DataType.array) {
            transformResult = v => v;
        } else {
            if (selection.columns.length !== 1) {
                throw new QueryError(`return type mismatch in function declared to return ${returns.name}`, '42P13');
            }
            const col = selection.columns[0];
            transformResult = (v, t) => v[0] ? col.get(v[0], t) : null;
        }


        // return compiled function
        return (...args: any[]) => {
            const { transaction } = executionCtx();
            const rows = [...selection.enumerate(transaction)];
            return transformResult(rows, transaction);
        }
    });
}