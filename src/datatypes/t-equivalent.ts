import { Evaluator } from '../evaluator';
import { TypeBase } from './datatype-base';
import { CastError, DataType, IEquivalentType, IType, nil, QueryError, typeDefToStr } from '../interfaces';
import { _ISchema, _IType } from '../interfaces-private';
import { Types } from './datatypes';

export class EquivalentType extends TypeBase<string> {

    private equiv: IType;

    constructor(private def: IEquivalentType) {
        super();
        if (typeof def.equivalentTo === 'string') {
            let eq = (Types as any)[def.equivalentTo];
            if (typeof eq === 'function') {
                eq = eq();
            }
            this.equiv = eq;
        } else {
            this.equiv = def.equivalentTo;
        }

        if (!this.equiv) {
            throw new Error(`Invalid equilvalent type`);
        }
    }

    get primary(): DataType {
        return this.def.name as any;
    }

    doCanCast(to: _IType) {
        return to.primary === this.equiv.primary;
    }

    doCast(value: Evaluator<string>, to: _IType<string>): Evaluator<any> | nil {
        return value;
    }

    prefer(type: _IType<any>): _IType | nil {
        return this;
    }

    doCanBuildFrom(from: _IType): boolean | nil {
        return from.primary === this.equiv.primary;
    }

    doBuildFrom(value: Evaluator<string>, from: _IType<string>): Evaluator<string> | nil {
        return value
            .setConversion(x => {
                if (!this.def.isValid(x)) {
                    throw new QueryError(`invalid input syntax for type ${typeDefToStr(this)}: ${x}`);
                }
                return x;
            }, val => ({ val, to: this.equiv.primary }));
    }
}
