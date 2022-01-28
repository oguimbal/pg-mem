import { QName, Statement, NodeLocation } from 'pgsql-ast-parser';
import { _ISchema, QueryError, QueryResult, _Transaction, StatementResult } from '../interfaces-private';

export function checkExistence(schema: _ISchema, name: QName, ifNotExists: boolean | undefined, act: () => void): boolean {
    // check if object exists
    const exists = schema.getObject(name, {
        skipSearch: true,
        nullIfNotFound: true
    });
    if (exists) {
        if (ifNotExists) {
            return false;
        }
        throw new QueryError(`relation "${name.name}" already exists`);
    }

    // else, perform operation
    act();
    return true;
}



export function resultNoData(op: string, p: Statement, t: _Transaction, ignored?: boolean): StatementResult {
    return {
        result: {
            command: op,
            fields: [],
            rowCount: 0,
            ignored,
            rows: [],
            location: locOf(p),
        },
        state: t,
    };
}

export function locOf(p: Statement): NodeLocation {
    return p._location ?? { start: 0, end: 0 };
}
