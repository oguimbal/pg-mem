import { IValue, _IIndex, _ISelection, _IType } from './interfaces-private';
import { DataType, CastError, IType } from './interfaces';
import moment from 'moment';
import hash from 'object-hash';
import { NotSupported } from './utils';
import { Evaluator, Value } from './valuetypes';

abstract class TypeBase<TRaw = any> implements _IType<TRaw> {

    abstract primary: DataType;
    doConvert?(value: Evaluator<TRaw>, to: _IType<TRaw>): Evaluator<any>;
    doCanConvert?(to: _IType<TRaw>): boolean;
    abstract doEquals(a: TRaw, b: TRaw): boolean;
    abstract doGt(a: TRaw, b: TRaw): boolean;
    abstract doLt(a: TRaw, b: TRaw): boolean;
    toString(): string {
        throw new Error('Method not implemented.');
    }

    equals(a: TRaw, b: TRaw): boolean {
        if (a === null || b === null) {
            return false;
        }
        return this.doEquals(a, b);
    }

    gt(a: TRaw, b: TRaw): boolean {
        if (a === null || b === null) {
            return false;
        }
        return this.doGt(a, b);
    }
    lt(a: TRaw, b: TRaw): boolean {
        if (a === null || b === null) {
            return false;
        }
        return this.doLt(a, b);
    }

    canConvert(_to: DataType | _IType<TRaw>): boolean {
        const to = makeType(_to);
        if (to === this) {
            return true;
        }
        return this.doCanConvert && this.doCanConvert(to);
    }

    convert(a: IValue<TRaw>, _to: DataType | _IType<any>): IValue<any> {
        const to = makeType(_to);
        if (to === this) {
            return a;
        }
        if (!this.canConvert(to) || !this.doConvert || !(a instanceof Evaluator)) {
            throw new CastError(this.primary, to.primary);
        }
        const converted = this.doConvert(a, to);
        if (!converted) {
            throw new CastError(this.primary, to.primary);
        }
        if (a.isConstant) {
            if (typeof converted.val === 'function') {
                converted.val = converted.val(null);
            }
            if (converted.val === null) {
                return Value.null();
            }
        }
        return converted;
    }
}


class TimestampType extends TypeBase<moment.Moment> {

    get primary(): DataType {
        return DataType.timestamp;
    }


    doEquals(a: any, b: any): boolean {
        return moment(a).diff(moment(b)) < 0.1;
    }
    doGt(a: any, b: any): boolean {
        return moment(a).diff(moment(b)) > 0;
    }
    doLt(a: any, b: any): boolean {
        return moment(a).diff(moment(b)) < 0;
    }
}

class NullType extends TypeBase<null> {

    get primary(): DataType {
        return DataType.null;
    }

    doConvert(value: Evaluator<any>, to: _IType): Evaluator<any> {
        return new Evaluator(to, null, 'null', 'null', null, null);
    }

    doCanConvert(to: _IType): boolean {
        return true;
    }

    doEquals(a: any, b: any): boolean {
        return false;
    }

    doGt(a: any, b: any): boolean {
        return false;
    }

    doLt(a: any, b: any): boolean {
        return false;
    }
}


class TextType extends TypeBase<string> {

    get primary(): DataType {
        return DataType.text;
    }

    doCanConvert(to: _IType): boolean {
        return to.primary === DataType.timestamp;
    }

    doConvert(value: Evaluator<string>, to: _IType) {
        switch (to.primary) {
            case DataType.timestamp:
                return new Evaluator(Types.timestamp, value.id, value.sql, value.hash, value.selection, raw => {
                    const got = value.get(raw);
                    return moment(got);
                });
        }
    }

    doEquals(a: any, b: any): boolean {
        return a === b;
    }

    doGt(a: any, b: any): boolean {
        return a > b;
    }

    doLt(a: any, b: any): boolean {
        return a < b;
    }
}

class BoolType extends TypeBase<boolean> {
    get primary(): DataType {
        return DataType.bool;
    }

    doEquals(a: any, b: any): boolean {
        return a === b;
    }

    doGt(a: any, b: any): boolean {
        return a < b;
    }
    doLt(a: any, b: any): boolean {
        return a < b;
    }
}

export class ArrayType extends TypeBase<any[]> {
    get primary(): DataType {
        return DataType.array;
    }

    constructor(readonly of: _IType)  {
        super();
    }

    doCanConvert(to: _IType) {
        return to instanceof ArrayType
            && to.canConvert(this.of);
    }

    doConvert(value: IValue, _to: _IType) {
        const to = _to as ArrayType;
        const valueType = value.type as ArrayType;
        return new Evaluator(to
            , value.id
            , value.sql
            , value.hash
            , value.selection
            , raw => {
                const arr = value.get(raw) as any[];
                return arr.map(x => Value.constant(x, valueType.of).convert(to.of).get(raw));
            });
    }

    doEquals(a: any[], b: any[]): boolean {
        if (a.length !== b.length) {
            return false;
        }
        for (let i = 0; i < a.length; i++) {
            if (!this.of.equals(a[i], b[i])) {
                return false;
            }
        }
        return true;
    }

    doGt(a: any[], b: any[]): boolean {
        const len = Math.min(a.length, b.length);
        for (let i = 0; i < len; i++) {
            if (this.of.gt(a[i], b[i])) {
                return true;
            }
        }
        return a.length > b.length;
    }

    doLt(a: any[], b: any[]): boolean {
        const len = Math.min(a.length, b.length);
        for (let i = 0; i < len; i++) {
            if (this.of.lt(a[i], b[i])) {
                return true;
            }
        }
        return a.length < b.length;
    }

}

export function makeType(to: DataType | _IType<any>): _IType<any> {
    if (typeof to === 'string') {
        if (!Types[to]) {
            throw new Error('Unsupported raw type: ' + to);
        }
        return Types[to];
    }
    return to;
}

type Ctors = {
    [key in DataType]?: _IType;
};
export const Types: Ctors = {
    [DataType.bool]: new BoolType(),
    [DataType.text]: new TextType(),
    [DataType.timestamp]: new TimestampType(),
    [DataType.null]: new NullType(),
}

const arrays = new Map<_IType, _IType>();

export function makeArray(of: _IType): _IType {
    let got = arrays.get(of);
    if (got) {
        return got;
    }
    arrays.set(of, got = new ArrayType(of));
    return got;
}