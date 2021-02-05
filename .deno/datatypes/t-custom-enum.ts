import { Evaluator } from '../valuetypes.ts';
import { TypeBase } from './datatype-base.ts';
import { CastError, DataType, nil, QueryError } from '../interfaces.ts';
import { _ISchema, _IType } from '../interfaces-private.ts';

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

    doCanBuildFrom(from: _IType): boolean | nil {
        return from.primary === DataType.text;
    }

    doBuildFrom(value: Evaluator<string>, from: _IType<string>): Evaluator<string> | nil {
        return value
            .setConversion((raw: string) => {
                if (!this.values.includes(raw)) {
                    throw new QueryError(`invalid input value for enum ${this.name}: "${raw}"`);
                }
                return raw;
            }
                , conv => ({ conv, toCenum: this.name }))
    }
}
