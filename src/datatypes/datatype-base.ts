import { Evaluator } from '../valuetypes';
import { CastError, DataType, IValue, nil, _IType } from '../interfaces-private';
import { makeType } from './datatypes';

export abstract class TypeBase<TRaw = any> implements _IType<TRaw> {

    abstract primary: DataType;
    abstract regTypeName: string | null;
    /** Can be casted to */
    doCanCast?(to: _IType<TRaw>): boolean | nil;

    /**
     * @see this.prefer() doc
      */
    doPrefer?(type: _IType<TRaw>): _IType | null;

    /**
     * @see this.canConvertImplicit() doc
     */
    doCanConvertImplicit?(to: _IType<TRaw>): boolean;

    /** Perform conversion */
    doCast?(value: Evaluator<TRaw>, to: _IType<TRaw>): Evaluator<any> | nil;

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
        if (a === null || b === null) {
            return null;
        }
        return this.doEquals(a, b);
    }

    gt(a: TRaw, b: TRaw): boolean | null {
        if (a === null || b === null) {
            return null;
        }
        return this.doGt(a, b);
    }
    lt(a: TRaw, b: TRaw): boolean | null {
        if (a === null || b === null) {
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
    prefer(type: DataType | _IType<TRaw>): _IType | nil {
        const to = makeType(type) as TypeBase;
        if (to === this) {
            return this;
        }
        if (this.doPrefer) {
            const ret = this.doPrefer(to);
            if (ret) {
                return ret;
            }
        }
        return to.doPrefer?.(this);
    }

    /**
     * Can constant literals be converted implicitely
     * (without a cast... i.e. you can use both values as different values of a case expression, for instance)
     **/
    canConvertImplicit(_to: DataType | _IType<TRaw>): boolean | nil {
        const to = makeType(_to);
        if (to === this) {
            return true;
        }
        return this.doCanConvertImplicit?.(to);
    }

    /** Can be explicitely casted to */
    canConvert(_to: DataType | _IType<TRaw>): boolean | nil {
        const to = makeType(_to);
        if (to === this) {
            return true;
        }
        return this.doCanCast?.(to);
    }

    /** Perform conversion */
    convert(a: IValue<TRaw>, _to: DataType | _IType<any>): IValue<any> {
        const to = makeType(_to);
        if (to === this) {
            return a;
        }
        if (!this.canConvert(to) || !this.doCast || !(a instanceof Evaluator)) {
            throw new CastError(this.primary, to.primary);
        }
        const converted = this.doCast(a, to);
        if (!converted) {
            throw new CastError(this.primary, to.primary);
        }
        return converted.setType(to);
    }

    constantConverter<TTarget>(_to: DataType | _IType<TTarget>): ((val: TRaw) => TTarget) {
        let current: TRaw;
        const ev = new Evaluator(this, null, null, null as any, null, () => current, {
            unpure: true,
        });
        const converted = this.convert(ev, _to);
        return (source: TRaw) => {
            current = source;
            return converted.get()
        };
    }
}