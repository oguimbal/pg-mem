import { DataType, getId, nil, QueryError, _IType } from '../interfaces-private.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../valuetypes.ts';
import { deepCompare, deepEqual } from '../utils.ts';


export class RecordType extends TypeBase<any> {

    get primary(): DataType {
        return DataType.record;
    }

    doEquals(a: any, b: any): boolean {
        return getId(a) === getId(b);
    }
}