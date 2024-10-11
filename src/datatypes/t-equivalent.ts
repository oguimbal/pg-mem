import { Evaluator } from '../evaluator';
import { TypeBase } from './datatype-base';
import { CastError, DataType, IEquivalentType, IType, nil, QueryError, typeDefToStr } from '../interfaces';
import { _ISchema, _IType } from '../interfaces-private';
import { Types } from './datatypes';

export class EquivalentType extends TypeBase<string> {

    private equiv: _IType;

    constructor(private def: IEquivalentType) {
        super(null);
        if (typeof def.equivalentTo === 'string') {
            let eq = (Types as any)[def.equivalentTo];
            if (typeof eq === 'function') {
                eq = eq();
            }
            this.equiv = eq;
        } else {
            this.equiv = def.equivalentTo as _IType;
        }

        if (!this.equiv) {
            throw new Error(`Invalid equilvalent type`);
        }
    }

    get primary(): DataType {
        return this.equiv.primary;
    }

    get primaryName(): string {
        return this.def.name;
    }

    get name(): string {
        return this.def.name;
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
        // return from.canCast(this.equiv);
        return this.equiv.canCast(from);
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
