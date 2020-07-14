import { DataType, _ISelection, SchemaField, IValue } from './interfaces-private';
import { TextValue, TimestampValue, allTypes } from './datatypes';
import { NotSupported } from './utils';



export class Column {
    constructor(selection: _ISelection, schema: SchemaField) {
        const ctor = allTypes[schema.type];
        if (!ctor) {
            throw new NotSupported('type ' + schema.type);
        }
        this.value = new ctor(schema.id
            , schema.id
            , schema.id
            , selection
            , raw => raw[schema.id]);
    }

    value: IValue;
}