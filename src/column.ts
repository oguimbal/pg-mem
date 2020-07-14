import { DataType, _ISelection, SchemaField, IValue } from './interfaces-private';
import { ValueCtor, TextValue, TimestampValue } from './datatypes';
import { NotSupported } from './utils';


type Ctors = { [key in DataType]: ValueCtor };
const types: Ctors = {
    [DataType.text]: TextValue,
    [DataType.timestamp]: TimestampValue,
} as any;

export class Column {
    constructor(selection: _ISelection, schema: SchemaField) {
        const ctor = types[schema.type];
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