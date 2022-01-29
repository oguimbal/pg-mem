import { DataType, getId, nil, QueryError, _IType } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { RecordCol } from './datatypes';

export class RecordType extends TypeBase<any> {

    constructor(readonly columns: readonly RecordCol[]) {
        super();
    }

    get primary(): DataType {
        return DataType.record;
    }

    doEquals(a: any, b: any): boolean {
        return getId(a) === getId(b);
    }
}
