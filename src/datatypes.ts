import { IValue, _IIndex, _ISelection } from './interfaces-private';
import { DataType, CastError } from './interfaces';
import moment from 'moment';
import hash from 'object-hash';

interface ValueCtor {
    new(id: string
        , sql: string
        , hash: string
        , selection: _ISelection
        , val: any | ((raw: any) => any)): ValueBase<any>;
    prototype: ValueBase<any>;
}

abstract class ValueBase<T> implements IValue {
    abstract type: DataType;
    abstract doConvert(to: DataType): ValueBase<any>;
    abstract canConvert(to: DataType): boolean;
    abstract doEquals(a: any, b: any): boolean;
    abstract doGt(a: any, b: any): boolean;
    abstract doLt(a: any, b: any): boolean;

    constructor(readonly id: string
        , readonly sql: string
        , readonly hash: string
        , readonly selection: _ISelection
        , public val: T | ((raw: any) => T)) {
    }

    get index() {
        return this.selection?.getIndex(this);
    }

    get isConstant(): boolean {
        return typeof this.val !== 'function';
    }

    get(raw: any): T {
        if (typeof this.val !== 'function') {
            return this.val;
        }
        return (this.val as ((raw: any) => T))(raw);
    }
    convert(to: DataType): IValue<any> {
        const converted = this.doConvert(to);
        if (this.isConstant) {
            if (typeof converted.val === 'function') {
                converted.val = converted.val(null);
            }
            if (converted.val === null) {
                return NullValue.constant();
            }
        }
        return converted;
    }

    setId(newId: string): IValue {
        if (this.id === newId) {
            return this;
        }
        if (this instanceof Wrapper) {
            return new Wrapper(newId, this.w);
        }
        return new Wrapper<T>(newId, this);
    }

    equals(a: any, b: any): boolean {
        if (a === null || b === null) {
            return false;
        }
        return this.doEquals(a, b);
    }

    gt(a: any, b: any): boolean {
        if (a === null || b === null) {
            return false;
        }
        return this.doGt(a, b);
    }
    lt(a: any, b: any): boolean {
        if (a === null || b === null) {
            return false;
        }
        return this.doLt(a, b);
    }
}

class Wrapper<T> extends ValueBase<T> {

    constructor(id: string, public w: ValueBase<T>) {
        super(id, w.sql, w.hash, w.selection, w.val);
    }
    get type(): DataType {
        return this.w.type;
    }

    doConvert(to: DataType): ValueBase<any> {
        const conv = this.w.convert(to);
        if (conv === this.w) {
            return this;
        }
        return new Wrapper(this.id, conv as ValueBase<any>);
    }

    canConvert(to: DataType): boolean {
        return this.w.canConvert(to);
    }

    doEquals(a: any, b: any): boolean {
        return this.w.doEquals(a, b);
    }

    doGt(a: any, b: any): boolean {
        return this.w.doGt(a, b);
    }

    doLt(a: any, b: any): boolean {
        return this.w.doLt(a, b);
    }
}

export class TimestampValue extends ValueBase<moment.Moment> {

    get type(): DataType {
        return DataType.timestamp;
    }

    canConvert(to: DataType): boolean {
        return to === DataType.timestamp;
    }

    doConvert(to: DataType) {
        if (to === DataType.timestamp) {
            return this;
        }
        throw new CastError(this.type, to);
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

export class NullValue extends ValueBase<string> {
    get type(): DataType {
        return null;
    }
    static constant(): IValue {
        return new NullValue(null, 'null', 'null', null, null);
    }

    doConvert(to: DataType): ValueBase<any> {
        return new allTypes[to](null, 'null', 'null', null, null);
    }

    canConvert(to: DataType): boolean {
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

export class TextValue extends ValueBase<string> {

    static constant(value: string): IValue {
        return new TextValue(null
            , `[${value}]`
            , value
            , null
            , value);
    }

    get type(): DataType {
        return DataType.text;
    }

    canConvert(to: DataType): boolean {
        switch (to) {
            case DataType.timestamp:
            case DataType.text:
                return true;
        }
        return false;
    }

    doConvert(to: DataType) {
        switch (to) {
            case DataType.text:
                return this;
            case DataType.timestamp:
                return new TimestampValue(this.id, this.sql, this.hash, this.selection, raw => {
                    const got = this.get(raw);
                    return moment(got);
                });
            default:
                throw new CastError(this.type, to);
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

export class BoolValue extends ValueBase<boolean> {

    static constant(value: boolean) {
        const str = value ? 'true' : 'false';
        return new BoolValue(null
            , str
            , str
            , null
            , () => value);
    }
    get type(): DataType {
        return DataType.bool;
    }

    canConvert(to: DataType): boolean {
        return to === DataType.bool;
    }

    doConvert(to: DataType) {
        if (to === DataType.bool) {
            return this;
        }
        throw new CastError(this.type, to);
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

export class IsNullValue extends ValueBase<boolean> {

    static of(leftValue: IValue, expectNull: boolean) {
        return new IsNullValue(null
            , leftValue.sql + ' IS NULL'
            , hash({ isNull: leftValue.hash })
            , leftValue.selection
            , expectNull ? (raw => {
                const left = leftValue.get(raw);
                return left === null;
            }) : (raw => {
                const left = leftValue.get(raw);
                return left !== null && left !== undefined;
            }));
    }

    get type(): DataType {
        return DataType.bool;
    }

    doConvert(to: DataType): ValueBase<any> {
        if (to === DataType.bool) {
            return this;
        }
        throw new CastError(this.type, to);
    }

    canConvert(to: DataType): boolean {
        return to === DataType.bool;
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


type Ctors = { [key in DataType]?: ValueCtor };
export const allTypes: Ctors = {
    [DataType.text]: TextValue,
    [DataType.timestamp]: TimestampValue,
};