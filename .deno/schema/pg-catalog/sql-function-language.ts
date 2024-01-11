import { _IDb, _ISchema, _Transaction, _IType, IValue, _Explainer, _ISelection, setId, getId } from '../../interfaces-private.ts';
import { parseSql } from '../../parser/parse-cache.ts';
import { QueryError, NotSupported, DataType } from '../../interfaces.ts';
import { Statement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { executionCtx as executionCtx, pushExecutionCtx, hasExecutionCtx, ExecCtx, randomString } from '../../utils.ts';
import { SelectExec } from '../../execution/select.ts';
import { withParameters } from '../../parser/context.ts';
import { buildParameterList } from '../../evaluator.ts';
import { StatementExec } from '../../execution/statement-exec.ts';
import { ArrayType } from '../../datatypes/index.ts';
import { RecordType } from '../../datatypes/t-record.ts';

let execId = 0;

export function registerSqlFunctionLanguage(db: _IDb) {
    db.registerLanguage('sql', ({ code, schema: _schema, args, returns: _returns }) => {
        const schema = _schema as _ISchema;
        const returns = _returns as _IType;
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
        let transformResult: (values: any[], t: _Transaction, execId: string) => any;
        if (!returns || returns.primary === DataType.null) {
            // returns null
            transformResult = () => null;
        } else if (ArrayType.matches(returns) && RecordType.matches(returns.of)) {
            // returns a table
            const transformItem = returns.of.transformItemFrom(executor.selection);
            if (!transformItem) {
                throw new QueryError(`return type mismatch in function declared to return record`, '42P13');
            }
            transformResult = (v, t, eid) => v?.map(x => {
                return transformItem(x, t, eid);
            });
        } else {
            // returns a single value
            const cols = executor.selection.columns;
            if (cols.length !== 1 || !cols[0].type.canConvertImplicit(returns)) {
                throw new QueryError(`return type mismatch in function declared to return ${returns.name}`, '42P13');
            }
            const col = cols[0].cast(returns);
            transformResult = (v, t) => v[0] ? col.get(v[0], t) : null;
        }


        // return compiled function
        const implem = (...args: any[]) => {
            const exec: ExecCtx = hasExecutionCtx() ?
                {
                    // if we have a parent execution context, use it.
                    // except for parameter values, that will be re-bound.
                    ...executionCtx(),
                    parametersValues: args,
                } : {
                    // else, create a brand new execution context.
                    // that is used when a pure function is called with constant arguments:
                    //  => function call will be reduced to a constant based on the
                    //    db state at the time of the statement begining.
                    schema,
                    transaction: db.data,
                    parametersValues: args,
                };
            const eid = 'fne' + execId++;
            // push a new execution context, to avoid leaking parent paramters in the function
            return pushExecutionCtx(exec, () => {
                const ret = executor.execute(exec.transaction);
                return transformResult(ret.result.rows, exec.transaction, eid);
            });
        };
        // hack to tell the expression visitor that
        // (implem as any)[fnAsSelectionColumns] = executor.selection.columns;
        return implem;
    });
}

// export const fnAsSelectionColumns = Symbol('asSelection');
