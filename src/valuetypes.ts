import { IValue, _IIndex, _ISelection, _IType } from './interfaces-private';
import { DataType, QueryError, CastError } from './interfaces';
import hash from 'object-hash';
import { Types, makeArray, makeType, ArrayType, isNumeric, singleSelection } from './datatypes';
import { Query } from './query';
import { buildCall } from './functions';


export class Evaluator<T = any> implements IValue<T> {

    constructor(
        readonly type: _IType<T>
        , readonly id: string
        , readonly sql: string
        , readonly hash: string
        , readonly origin: _ISelection
        , public val: T | ((raw: any) => T)) {
    }

    setType(type: _IType) {
        if (this.type === type) {
            return this;
        }
        return new Evaluator<T>(
            type
            , this.id
            , this.sql
            , this.hash
            , this.origin
            , this.val
        );
    }



    setConversion(converter: (val: any) => any
        , sqlConv: (sql: string) => string
        , hashConv: (hash: string) => any) {
        return new Evaluator<T>(
            this.type
            , this.id
            , sqlConv(this.sql)
            , hash(hashConv(this.hash))
            , this.origin
            , raw => {
                const got = converter(raw)
                if (got === null || got === undefined) {
                    return null;
                }
                return this.get(got);
            }
        ).asConstant(this.isConstant);
    }

    setWrapper(wrap: (val: any) => any) {
        return new Evaluator<T>(
            this.type
            , this.id
            , this.sql
            , this.hash
            , this.origin
            , raw => {
                const got = wrap(raw)
                if (got === null || got === undefined) {
                    return null;
                }
                return this.get(got);
            }
        ).asConstant(this.isConstant);
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
            , this.origin
            , this.val
        );
    }

    get index() {
        return this.origin?.getIndex(this);
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

    asConstant(perform = true) {
        if (!perform || typeof this.val !== 'function') {
            return this;
        }
        return new Evaluator(this.type
            , this.id
            , this.sql
            , this.hash
            , this.origin
            , this.get(null));
    }

    canConvert(to: DataType | _IType<T>): boolean {
        return this.type.canConvert(to);
    }

    convert<T = any>(to: DataType | _IType<T>): IValue<T> {
        return this.type.convert(this, to);
    }

    toString() {
        return this.sql;
    }
}

// export class ArrayEvaluator<T> implements IValue {

//     constructor(
//         readonly type: _IType<T>
//         , readonly id: string
//         , readonly sql: string
//         , readonly hash: string
//         , readonly selection: _ISelection
//         , public val: T | ((raw: any) => T)) {
//     }

//     get index() {
//         return this.selection?.getIndex(this);
//     }

//     get isConstant(): boolean {
//         return typeof this.val !== 'function';
//     }

//     get(raw: any): T {
//         if (typeof this.val !== 'function') {
//             return this.val;
//         }
//         return (this.val as ((raw: any) => T))(raw);
//     }

//     asConstant(perform = true) {
//         if (!perform || typeof this.val !== 'function') {
//             return this;
//         }
//         return new Evaluator(this.type
//             , this.id
//             , this.sql
//             , this.hash
//             , this.selection
//             , this.get(null));
//     }


//     setId(newId: string): IValue {
//         if (this.id === newId) {
//             return this;
//         }
//         return new Evaluator<T>(
//             this.type
//             , newId
//             , this.sql
//             , this.hash
//             , this.selection
//             , this.val
//         );
//     }

//     canConvert(to: DataType | _IType<T>): boolean {
//         return this.type.canConvert(to);
//     }

//     convert<T = any>(to: DataType | _IType<T>): IValue<T> {
//         return this.type.convert(this, to);
//     }
// }


export const Value = {
    null(): IValue {
        return new Evaluator(Types.null, null, 'null', 'null', null, null);
    },
    text(value: string, length: number = null) {
        return new Evaluator(
            Types.text(length)
            , null
            , `[${value}]`
            , value
            , null
            , value);
    },
    number(value: number, type = Types.float) {
        return new Evaluator(
            type
            , null
            , `[${value}]`
            , value.toString(10)
            , null
            , value);
    },
    function(value: string, args: IValue[]) {
        return buildCall(value, args);
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
    /** @deprecated Use with care */
    constant(_type: DataType | _IType, value: any) {
        const type = value === null ? Types.null : makeType(_type);
        return new Evaluator(type
            , null
            , null
            , null
            , null
            , value);
    },
    in(value: IValue, array: IValue, inclusive: boolean) {
        if (!value) {
            throw new Error('Argument null');
        }
        if (array.type.primary !== DataType.array) {
            throw new QueryError('Expecting element list');
        }
        const of = (array.type as ArrayType).of;
        return new Evaluator(
            Types.bool
            , null
            , value.sql + ' IN ' + array.sql
            , hash({ val: value.hash, in: array.hash })
            , value.origin
            , raw => {
                const rawValue = value.get(raw);
                const rawArray = array.get(raw);
                if (!Array.isArray(rawArray)) {
                    return false;
                }
                const has = rawArray.some(x => of.equals(rawValue, x));
                return inclusive ? has : !has;
            })
            .asConstant(value.isConstant && array.isConstant);
    },
    isNull(leftValue: IValue, expectNull: boolean) {
        return new Evaluator(
            Types.bool
            , null
            , `${leftValue.sql} IS${expectNull ? '' : ' NOT'} NULL`
            , hash({ isNull: leftValue.hash, expectNull })
            , leftValue.origin
            , expectNull ? (raw => {
                const left = leftValue.get(raw);
                return left === null;
            }) : (raw => {
                const left = leftValue.get(raw);
                return left !== null && left !== undefined;
            })).asConstant(leftValue.isConstant);
    },
    isTrue(leftValue: IValue, expectTrue: boolean) {
        leftValue = leftValue.convert(Types.bool);
        return new Evaluator(
            Types.bool
            , null
            , `${leftValue.sql} IS${leftValue ? '' : ' NOT'} TRUE`
            , hash({ isTrue: leftValue.hash, expectTrue })
            , leftValue.origin
            , expectTrue ? (raw => {
                const left = leftValue.get(raw);
                return left === true; // never returns null
            }) : (raw => {
                const left = leftValue.get(raw);
                return !(left === true); //  never returns null
            })).asConstant(leftValue.isConstant);
    },
    isFalse(leftValue: IValue, expectFalse: boolean) {
        leftValue = leftValue.convert(Types.bool);
        return new Evaluator(
            Types.bool
            , null
            , `${leftValue.sql} IS${leftValue ? '' : ' NOT'} FALSE`
            , hash({ isFalse: leftValue.hash, expectFalse })
            , leftValue.origin
            , expectFalse ? (raw => {
                const left = leftValue.get(raw);
                return left === false; // never returns null
            }) : (raw => {
                const left = leftValue.get(raw);
                return !(left === false); //  never returns null
            })).asConstant(leftValue.isConstant);
    },
    negate(value: IValue) {
        if (!isNumeric(value.type)) {
            throw new QueryError('Can only apply "-" unary operator to numeric types');
        }
        return (value as Evaluator)
            .setConversion(x => -x, x => '-(' + x + ')', x => ({neg: x}));
    },
    array(values: IValue[]) {
        if (!values.length) {
            throw new QueryError('Expecting some value in list');
        }
        const type = values.reduce((t, v) => {
            if (v.canConvert(t)) {
                return t;
            }
            if (!t.canConvert(v.type)) {
                throw new CastError(t.primary, v.type.primary);
            }
            return v.type;
        }, Types.null);
        // const sel = values.find(x => !!x.selection)?.selection;
        const converted = values.map(x => x.convert(type));
        return new Evaluator(makeArray(type)
            , null
            , '(' + converted.map(x => x.sql).join(', ') + ')'
            , hash(converted.map(x => x.hash))
            , singleSelection(converted)
            , raw => {
                const arr = values.map(x => x.get(raw));
                return arr;
            }).asConstant(!values.some(x => !x.isConstant))
    }
} as const;