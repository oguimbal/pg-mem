import { DataType, getId, nil, QueryError, _IType } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../valuetypes';
import { deepCompare, deepEqual } from '../utils';


export class RecordType extends TypeBase<any> {

    get primary(): DataType {
        return DataType.record;
    }

    doEquals(a: any, b: any): boolean {
        return getId(a) === getId(b);
    }
}