import { IValue, _IIndex, _ISelection, _IType } from './interfaces-private';
import { DataType } from './interfaces';
import hash from 'object-hash';
import { Types, MakeArray } from './datatypes';


export class Evaluator<T> implements IValue<T> {
    constructor(
        readonly type: _IType<T>
        , readonly id: string
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


    setId(newId: string): IValue {
        if (this.id === newId) {
            return this;
        }
        return new Evaluator<T>(
            this.type
            , newId
            , this.sql
            , this.hash
            , this.selection
            , this.val
        );
    }

    canConvert(to: DataType | _IType<T>): boolean {
        return this.type.canConvert(to);
    }

    convert<T = any>(to: DataType | _IType<T>): IValue<T> {
        return this.type.convert(this, to);
    }
}


export const Value = {
    null(): IValue {
        return new Evaluator(Types.null, null, 'null', 'null', null, null);
    },
    text(value: string) {
        return new Evaluator(
            Types.text
            , null
            , `[${value}]`
            , value
            , null
            , value);
    },
    bool(value: boolean) {
        const str = value ? 'true' : 'false';
        return new Evaluator(
            Types.bool
            , null
            , str
            , str
            , null
            , value);
    },
    isNull(leftValue: IValue, expectNull: boolean) {
        return new Evaluator(
            Types.bool
            , null
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
    },
    array(values: IValue[]) {
        const type = null; // todo detect & cast
        debugger;
        throw new Error('todo');
        // return new Val(MakeArray(type)
        //     , null)
    }
} as const;