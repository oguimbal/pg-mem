import { QName, Statement, NodeLocation, toSql } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { _ISchema, QueryError, _Transaction, _IDb } from '../interfaces-private.ts';

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



export function locOf(p: Statement): NodeLocation {
    return p._location ?? { start: 0, end: 0 };
}

export abstract class ExecHelper {
    constructor(private statement: Statement) {
    }

    protected noData(t: _Transaction, name?: string) {
        return {
            result: {
                command: name ?? this.statement.type.toUpperCase(),
                fields: [],
                rowCount: 0,
                rows: [],
                location: locOf(this.statement),
            },
            state: t,
        };
    }
}
