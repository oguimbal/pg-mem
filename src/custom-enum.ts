import { Evaluator } from './valuetypes';
import { TypeBase } from './datatypes/datatype-base';
import { DataType, nil } from './interfaces';
import { IValue, Reg, _ICustomType, _ISchema, _IType } from './interfaces-private';

export class CustomEnumType extends TypeBase<string> implements _ICustomType {

    reg!: Reg;

    get type(): 'type' {
        return 'type';
    }

    get primary(): DataType {
        return this.name as any;
    }

    get regTypeName(): string | null {
        return this.name;
    }

    constructor(readonly schema: _ISchema, readonly name: string, readonly values: string[]) {
        super();
    }

    install() {
        this.reg = this.schema._reg_register(this, 'type');
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
