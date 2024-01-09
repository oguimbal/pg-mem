import { Evaluator } from '../evaluator';
import { TypeBase } from './datatype-base';
import { DataType, nil, QueryError } from '../interfaces';
import {_IRelation, _ISchema, _IType, _Transaction} from '../interfaces-private';

export function asEnum(o: _IRelation | null): CustomEnumType  {
    if (o && o.type === 'type' && o instanceof CustomEnumType) {
        return o;
    }
    throw new QueryError(`"${o?.name}" is not a enum`);
}
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


    drop(t: _Transaction): void {
        this.schema._unregisterType(this);
    }
}
