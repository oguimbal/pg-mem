import { _IDb, _ISchema, _Transaction, _IType } from '../../interfaces-private';
import { parseSql } from '../../parser/parse-cache';
import { QueryError, NotSupported, DataType } from '../../interfaces';
import { Statement } from 'pgsql-ast-parser';
import { executionCtx as executionCtx, fromEntries, pushExecutionCtx } from '../../utils';
import { SelectExec } from '../../execution/select';
import { withParameters, withSelection, withStatement } from '../../parser/context';
import { Value, buildParameterList } from '../../evaluator';
import { StatementExec } from '../../execution/statement-exec';

export function registerSqlFunctionLanguage(db: _IDb) {
    db.registerLanguage('sql', ({ code, schema: _schema, args, returns }) => {
        const schema = _schema as _ISchema;
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

        // build parameter list
        const parameterList = buildParameterList('', args);


        // push and push parameters, and a new build context, to avoid leaking parent context in function body
        // ... then, visit & compile tree
        const statement = new StatementExec(schema, parsed, code);
        const executor = withParameters(parameterList, () => statement.compile());
        if (!(executor instanceof SelectExec)) {
            throw new NotSupported(`Unsupported statement type in function: ${parsed.type}`);
        }


        // todo: prepare statement here, to avoid optimization on each call.


        // get the result transformer, based on the expected function output type
        let transformResult: (values: any[], t: _Transaction) => any;
        if (!returns || returns.primary === DataType.null) {
            transformResult = () => null;
        } else if (returns.primary === DataType.array) {
            transformResult = v => v;
        } else {
            if (executor.selection.columns.length !== 1) {
                throw new QueryError(`return type mismatch in function declared to return ${returns.name}`, '42P13');
            }
            const col = executor.selection.columns[0];
            transformResult = (v, t) => v[0] ? col.get(v[0], t) : null;
        }


        // return compiled function
        return (...args: any[]) => {
            const exec = executionCtx();
            // push a new execution context, to avoid leaking parent paramters in the function
            return pushExecutionCtx({
                ...exec,
                parametersValues: args,
            }, () => {
                const ret = executor.execute(exec.transaction);
                return transformResult(ret.result.rows, exec.transaction);
            })
        }
    });
}