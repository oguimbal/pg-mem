import { Evaluator } from '../evaluator';
import { CastError, DataType, IValue, nil, Reg, TR, _ISchema, _IType, _RelationBase, _Transaction } from '../interfaces-private';
import { ArrayType } from './datatypes';
import { isType, nullIsh } from '../utils';
import objectHash from 'object-hash';
import { QueryError } from '../interfaces';

let regCnt = 0;

export function regGen(): Reg {
    return {
        classId: ++regCnt,
        typeId: ++regCnt,
    };
}

export abstract class TypeBase<TRaw = any> implements _IType<TRaw>, _RelationBase {
    get [isType.TAG]() {
        return true;
    }

    readonly reg: Reg;

    get type(): 'type' {
        return 'type';
    }

    constructor() {
        this.reg = regGen();
    }

    private _asArray?: _IType<TRaw[]>;
    private _asList?: _IType<TRaw[]>;

    abstract primary: DataType;
    get name(): string {
        return this.primary;
    }


    /** Compute a custom unicty hash for a non null value */
    doGetHash?(value: TRaw): string | number;

    /** Can be casted to */
    doCanCast?(to: _IType<TRaw>): boolean | nil;

    /** Can be built to from (inverse of doCanCast()) */
    doCanBuildFrom?(from: _IType): boolean | nil;

    /**
     * @see this.prefer() doc
      */
    doPrefer?(type: _IType<TRaw>): _IType | null;

    /**
     * @see this.canConvertImplicit() doc
     */
    doCanConvertImplicit?(to: _IType<TRaw>): boolean;

    /** Perform conversion from this type to given type */
    doCast?(value: Evaluator<TRaw>, to: _IType<TRaw>): Evaluator<any> | nil;

    /** Perform conversion  given type to this type (inverse of doCast()) */
    doBuildFrom?(value: Evaluator, from: _IType): Evaluator<TRaw> | nil;

    doEquals(a: TRaw, b: TRaw): boolean {
        return a === b;
    }

    doGt(a: TRaw, b: TRaw): boolean {
        return a > b;
    }

    doLt(a: TRaw, b: TRaw): boolean {
        return a < b;
    }
    toString(): string {
        throw new Error('Method not implemented.');
    }

    equals(a: TRaw, b: TRaw): boolean | null {
        if (nullIsh(a) || nullIsh(b)) {
            return null;
        }
        return this.doEquals(a, b);
    }

    gt(a: TRaw, b: TRaw): boolean | null {
        if (nullIsh(a) || nullIsh(b)) {
            return null;
        }
        return this.doGt(a, b);
    }
    lt(a: TRaw, b: TRaw): boolean | null {
        if (nullIsh(a) || nullIsh(b)) {
            return null;
        }
        return this.doLt(a, b);
    }

    ge(a: TRaw, b: TRaw): boolean | null {
        return this.gt(a, b) || this.equals(a, b);
    }

    le(a: TRaw, b: TRaw): boolean | null {
        return this.lt(a, b) || this.equals(a, b);
    }

    /**
     * When performing 'a+b', will be given 'b' type,
     * this returns the prefered resulting type, or null if they are not compatible
      */
    prefer(to: _IType<TRaw>): _IType | nil {
        if (to === this) {
            return this;
        }
        if (this.doPrefer) {
            const ret = this.doPrefer(to);
            if (ret) {
                return ret;
            }
        }
        return (to as TypeBase).doPrefer?.(this);
    }

    /**
     * Can constant literals be converted implicitely
     * (without a cast... i.e. you can use both values as different values of a case expression, for instance)
     **/
    canConvertImplicit(to: _IType<TRaw>): boolean | nil {
        if (to === this) {
            return true;
        }
        return this.doCanConvertImplicit?.(to);
    }

    /** Can be explicitely casted to */
    canCast(to: _IType<TRaw>): boolean | nil {
        if (to === this) {
            return true;
        }

        // ask the target type if it know how to build itself from this
        if ((to as TypeBase).doCanBuildFrom?.(this)) {
            return true;
        }

        // asks this type if it knows how to convert itself to target
        if (this.doCanCast?.(to)) {
            return true;
        }

        return false;
    }

    /** Perform cast */
    cast(_a: IValue<TRaw>, _to: _IType<any>): IValue<any> {
        return this._convert(_a, _to, (a, to) => {
            if (!this.doCanCast?.(to) || !this.doCast) {
                throw new CastError(this.primary, to.primary);
            }
            return this.doCast(a, to);
        })
    }

    /** Perform implicit conversion */
    convertImplicit(_a: IValue<TRaw>, _to: _IType<any>): IValue<any> {
        return this._convert(_a, _to, (a, to) => {
            if (!this.doCanConvertImplicit?.(to) || !this.doCast) {
                throw new CastError(this.primary, to.primary);
            }
            return this.doCast(a, to);
        })
    }

    private _convert(a: IValue<TRaw>, _to: _IType<any>, perform: (a: Evaluator, to: _IType) => Evaluator<any> | nil): IValue<any> {
        const to = _to as TypeBase;
        if (to === this) {
            return a;
        }
        if (!(a instanceof Evaluator)) {
            throw new CastError(this.primary, to.primary);
        }

        let converted: Evaluator | nil;
        if (to.doCanBuildFrom?.(this)) {
            if (!to.doBuildFrom) {
                throw new CastError(this.primary, to.primary);
            }
            converted = to.doBuildFrom(a, this);
        } else {
            converted = perform(a, to);
        }

        if (!converted) {
            throw new CastError(this.primary, to.primary);
        }
        return converted.setType(to);
    }

    asArray(): _IType<TRaw[]> {
        if (this._asArray) {
            return this._asArray;
        }
        return this._asArray = new ArrayType(this, false);
    }

    asList(): _IType<TRaw[]> {
        if (this._asList) {
            return this._asList;
        }
        return this._asList = new ArrayType(this, true);
    }

    hash(value: any): string | number | null {
        if (nullIsh(value)) {
            return null;
        }
        if (this.doGetHash) {
            return this.doGetHash(value);
        }
        if (typeof value === 'number') {
            return value;
        }
        return objectHash(value);
    }

    drop(t: _Transaction): void {
        throw new QueryError('drop type not implemented');
    }

}
