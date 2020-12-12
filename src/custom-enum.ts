import { Evaluator } from './valuetypes';
import { TypeBase } from './datatypes/datatype-base';
import { DataType, nil } from './interfaces';
import { _ISchema, _IType } from './interfaces-private';

export class CustomEnumType extends TypeBase<string> {

    get primary(): DataType {
        return this.name as any;
    }

    get name(): string {
        return this._name;
    }

    constructor(readonly schema: _ISchema
        , private readonly _name: string
        , readonly values: string[]) {
        super();
    }

    install() {
        this.schema._registerType(this);
    }

    doCanCast(to: _IType) {
        return to.primary === DataType.text;
    }

    doCast(value: Evaluator<string>, to: _IType<string>): Evaluator<any> | nil {
        return value;
    }

    prefer(type: _IType<any>): _IType | nil {
        return this;
    }
}
