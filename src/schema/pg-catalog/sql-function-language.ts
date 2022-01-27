import { _IDb, _ISchema } from '../../interfaces-private';
import { parseSql } from '../../parser/parse-cache';
import { QueryError, NotSupported } from '../../interfaces';
import { Statement } from 'pgsql-ast-parser';
import { watchUse } from '../../utils';

export function registerSqlFunctionLanguage(db: _IDb) {
    db.registerLanguage('sql', ({ code, schema }) => {
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
        const { checked: p, check } = db.options.noAstCoverageCheck
            ? { checked: parsed, check: null }
            : watchUse(parsed);

        // const selection = (schema as _ISchema).buildSelect(p);

        check?.();

        // return compiled function
        return (...args: any[]) => {
            // selection.enumerate(currentTransaction())


            // check?.();
            // reutrn return;
        }
    });
}