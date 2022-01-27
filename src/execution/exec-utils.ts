import { QName } from 'pgsql-ast-parser';
import { _ISchema, QueryError } from '../interfaces-private';

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
