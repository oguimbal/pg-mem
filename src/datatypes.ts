import { IValue, _IIndex, _ISelection } from './interfaces-private';
import { DataType, CastError } from './interfaces';
import moment from 'moment';

export interface ValueCtor {
    new(id: string
        , sql: string
        , hash: string
        , selection: _ISelection
        , val: any | ((raw: any) => any)): IValue;
    prototype: IValue;
}

abstract class ValueBase<T> implements IValue {
    abstract type: DataType;

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



    protected abstract doConvert(to: DataType): ValueBase<any>;

    abstract canConvert(to: DataType): boolean;
    abstract equals(a: any, b: any): boolean;
    abstract gt(a: any, b: any): boolean;
    abstract lt(a: any, b: any): boolean;
}

class Wrapper<T> extends ValueBase<T> {

    constructor(id: string, public w: ValueBase<T>) {
        super(id, w.sql, w.hash, w.selection, w.val);
    }
    get type(): DataType {
        return this.w.type;
    }

    protected doConvert(to: DataType): ValueBase<any> {
        const conv = this.w.convert(to);
        if (conv === this.w) {
            return this;
        }
        return new Wrapper(this.id, conv as ValueBase<any>);
    }

    canConvert(to: DataType): boolean {
        return this.w.canConvert(to);
    }

    equals(a: any, b: any): boolean {
        return this.w.equals(a, b);
    }

    gt(a: any, b: any): boolean {
        return this.w.gt(a, b);
    }

    lt(a: any, b: any): boolean {
        return this.w.lt(a, b);
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

    equals(a: any, b: any): boolean {
        if (!a !== !b) {
            return false;
        }
        if (!a && !b) {
            return true;
        }
        return moment(a).diff(moment(b)) < 0.1;
    }

    gt(a: any, b: any): boolean {
        if (!a !== !b) {
            return false;
        }
        if (!a && !b) {
            return true;
        }
        return moment(a).diff(moment(b)) > 0;
    }
    lt(a: any, b: any): boolean {
        if (!a !== !b) {
            return false;
        }
        if (!a && !b) {
            return true;
        }
        return moment(a).diff(moment(b)) < 0;
    }
}

export class TextValue extends ValueBase<string> {

    static constant(value: string) {
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
    }

    protected doConvert(to: DataType) {
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

    equals(a: any, b: any): boolean {
        return a === b;
    }

    gt(a: any, b: any): boolean {
        return a > b;
    }

    lt(a: any, b: any): boolean {
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

    protected doConvert(to: DataType) {
        if (to === DataType.bool) {
            return this;
        }
        throw new CastError(this.type, to);
    }

    equals(a: any, b: any): boolean {
        return a === b;
    }

    gt(a: any, b: any): boolean {
        return a < b;
    }
    lt(a: any, b: any): boolean {
        return a < b;
    }

}