import { IValue, _IIndex, _ISelection, _IType, _Transaction, _Explainer, _ExprExplanation, _ISchema } from './interfaces-private.ts';
import { DataType, QueryError, CastError, nil, ISchema } from './interfaces.ts';
import hash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';
import { Types, ArrayType, isNumeric } from './datatypes/index.ts';
import { buildCall } from './function-call.ts';
import { nullIsh } from './utils.ts';
import { QName } from 'https://deno.land/x/pgsql_ast_parser@9.2.2/mod.ts';


export class Evaluator<T = any> implements IValue<T> {

    readonly isConstantLiteral: boolean;
    readonly usedColumns = new Set<IValue>();
    readonly forceNotConstant?: boolean;

    get index(): _IIndex | nil {
        return this.origin?.getIndex(this);
    }

    get isConstant(): boolean {
        return !this.usedColumns.size && !this.forceNotConstant;
    }

    get isConstantReal(): boolean {
        return typeof this.val !== 'function';
    }

    origin: _ISelection | nil;

    get isAny(): boolean {
        return this.opts?.isAny ?? false;
    }

    constructor(
        readonly owner: _ISchema
        , readonly type: _IType<T>
        , readonly id: string | nil
        , readonly hash: string
        , dependencies: IValue | IValue[] | nil
        , public val: nil | Object | number | string | Date | ((raw: any, transaction: _Transaction | nil) => any)
        , private opts?: {
            isAny?: boolean;
            isColumnOf?: _ISelection;
            forceNotConstant?: boolean;
            unpure?: boolean;
        }) {
        this.isConstantLiteral = typeof val !== 'function';
        if (opts?.forceNotConstant) {
            this.forceNotConstant = true;
        }

        // fetch columns to depend on
        let depArray: IValue[] | undefined = undefined;
        let hasNotConstant = false;
        if (dependencies) {
            if (!Array.isArray(dependencies)) {
                depArray = [dependencies];
                this.usedColumns = dependencies.usedColumns as Set<IValue>;
                hasNotConstant = !dependencies.isConstant;
                this.origin = dependencies.origin;
            } else {
                this.usedColumns = new Set();
                for (const d of dependencies) {
                    if (d.origin) {
                        if (this.origin && d.origin && this.origin !== d.origin) {
                            throw new Error('You cannot evaluate an expression which is coming from multiple origins');
                        }
                        this.origin = d.origin;
                    }
                    if (!d.isConstant) {
                        hasNotConstant = true;
                    }
                    for (const u of d.usedColumns) {
                        this.usedColumns.add(u);
                    }
                }
            }
        }

        if (opts?.isColumnOf) {
            this.usedColumns.add(this);
            this.origin = opts.isColumnOf;
            delete opts.isColumnOf;
        }
        if (hasNotConstant && !this.usedColumns.size) {
            this.forceNotConstant = true;
        }

        if (!this.usedColumns.size // no used columns
            && !this.origin
            && !this.opts?.unpure
            && !this.forceNotConstant
            && !depArray?.some(x => !x.isConstantReal)  // all real constant dependencies
        ) {
            // no dependency => this is a real constant => evaluate it.
            if (typeof this.val === 'function') {
                this.val = this.val(null, null);
            }
        }
    }

    setType(type: _IType) {
        if (this.type === type) {
            return this;
        }
        return new Evaluator<T>(
            this.owner
            , type
            , this.id
            , this.hash
            , this
            , this.val
            , this.opts
        );
    }



    setConversion(converter: (val: T, t: _Transaction | nil) => any
        , hashConv: (hash: string) => any) {
        return new Evaluator<T>(
            this.owner
            , this.type
            , this.id
            , hash(hashConv(this.hash))
            , this
            , (raw, t) => {
                let got = this.get(raw, t);
                if (nullIsh(got)) {
                    return null;
                }
                if (!this.isAny) {
                    return converter(got, t);
                }
                if (!Array.isArray(got)) {
                    throw new QueryError('Unexpected use of ANY()');
                }
                return (got as any[]).map(x => converter(x, t));
            }
            , this.opts
        );
    }

    setOrigin(origin: _ISelection): IValue<T> {
        const ret = this.clone();
        ret.origin = origin;
        return ret;
    }

    clone(): Evaluator<T> {
        return new Evaluator<T>(
            this.owner
            , this.type
            , this.id
            , this.hash
            , this
            , this.val
            , this.opts
        );
    }

    map<TNew>(mapper: (val: T) => TNew, newType?: _IType<TNew>): IValue<TNew> {
        if (this.isAny) {
            throw new QueryError('Unexpected use of ANY()');
        }
        const ret = new Evaluator<TNew>(
            this.owner
            , (newType ?? this.type) as _IType
            , this.id
            , this.hash
            , this
            , (raw, t) => {
                const got = this.get(raw, t);
                if (nullIsh(got)) {
                    return null;
                }
                return mapper(got);
            }
            , this.opts
        );
        ret.origin = this.origin;
        return ret;
    }

    setWrapper<TNew>(newOrigin: _ISelection, unwrap: (val: T) => TNew, newType?: _IType<TNew>): IValue<TNew> {
        if (this.isAny) {
            throw new QueryError('Unexpected use of ANY()');
        }
        const ret = new Evaluator<TNew>(
            this.owner
            , (newType ?? this.type) as _IType
            , this.id
            , this.hash
            , this
            , (raw, t) => {
                const got = unwrap(raw)
                if (nullIsh(got)) {
                    return null;
                }
                return this.get(got, t);
            }
            , this.opts
        );
        ret.origin = newOrigin;
        return ret;
    }

    setId(newId: string): IValue {
        if (this.id === newId) {
            return this;
        }
        return new Evaluator<T>(
            this.owner
            , this.type
            , newId
            , this.hash
            , this
            , this.val
            , this.opts
        );
    }

    get(): T;
    get(raw: any, t: _Transaction | nil): T;
    get(raw?: any, t?: _Transaction): T {
        if ((nullIsh(raw) || !t) && !this.isConstant) {
            throw new Error('Cannot be evaluated as constant');
        }
        if (typeof this.val !== 'function') {
            return this.val as any;
        }
        return this.val(raw, t)
    }

    canCast(to: _IType<T>): boolean {
        return !!this.type.canCast(to);
    }

    cast<T = any>(to: _IType<T>): IValue<T> {
        return this.type.cast(this, to);
    }

    convertImplicit<T = any>(to: _IType<T>): IValue<T> {
        return this.type.convertImplicit(this, to);
    }


    explain(e: _Explainer): _ExprExplanation {
        if (!this.origin) {
            return {
                constant: true,
            }
        }
        return {
            on: e.idFor(this.origin),
            col: this.id ?? '<complex expression>',
        };
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

//     canCast(to: DataType | _IType<T>): boolean {
//         return this.type.canCast(to);
//     }

//     cast<T = any>(to: DataType | _IType<T>): IValue<T> {
//         return this.type.cast(this, to);
//     }
// }


export const Value = {
    null(owner: _ISchema, ofType?: _IType): IValue {
        return new Evaluator(owner, ofType ?? Types.null, null, 'null', null, null, undefined);
    },
    text(owner: _ISchema, value: string, length: number | nil = null): IValue {
        return new Evaluator(
            owner
            , Types.text(length)
            , null
            , value
            , null
            , value);
    },
    number(owner: _ISchema, value: number, type = Types.float): IValue {
        return new Evaluator(
            owner
            , type
            , null
            , value.toString(10)
            , null
            , value);
    },
    function(schema: _ISchema, value: string | QName, args: IValue[]): IValue {
        return buildCall(schema, value, args);
    },
    bool(owner: _ISchema, value: boolean): IValue {
        const str = value ? 'true' : 'false';
        return new Evaluator(
            owner
            , Types.bool
            , null
            , str
            , null
            , value);
    },
    /** @deprecated Use with care */
    constant(owner: _ISchema, _type: _IType, value: any): IValue {
        const type = nullIsh(value)
            ? Types.null
            : _type;
        return new Evaluator(
            owner
            , type
            , null
            , (null as any)
            , null
            , value);
    },
    in(owner: _ISchema, value: IValue, array: IValue, inclusive: boolean): IValue {
        if (!value) {
            throw new Error('Argument null');
        }
        if (array.type.primary !== DataType.list && array.type) {
            array = Value.list(owner, [array]);
        }
        const of = (array.type as ArrayType).of;
        return new Evaluator(
            owner
            , Types.bool
            , null
            , hash({ val: value.hash, in: array.hash })
            , [value, array]
            , (raw, t) => {
                const rawValue = value.get(raw, t);
                const rawArray = array.get(raw, t);
                if (!Array.isArray(rawArray)) {
                    return false;
                }
                const has = rawArray.some(x => of.equals(rawValue, x));
                return inclusive ? has : !has;
            });
    },
    isNull(owner: _ISchema, leftValue: IValue, expectNull: boolean): IValue {
        return new Evaluator(
            owner
            , Types.bool
            , null
            , hash({ isNull: leftValue.hash, expectNull })
            , leftValue
            , (raw, t) => {
                const left = leftValue.get(raw, t);
                // check that result is null (will never return NULL)
                const ret = nullIsh(left);
                return expectNull ? ret : !ret;
            })
    },
    isTrue(owner: _ISchema, leftValue: IValue, expectTrue: boolean): IValue {
        leftValue = leftValue.cast(Types.bool);
        return new Evaluator(
            owner
            , Types.bool
            , null
            , hash({ isTrue: leftValue.hash, expectTrue })
            , leftValue
            , expectTrue ? ((raw, t) => {
                const left = leftValue.get(raw, t);
                return left === true; // never returns null
            }) : ((raw, t) => {
                const left = leftValue.get(raw, t);
                return !(left === true); //  never returns null
            }));
    },
    isFalse(owner: _ISchema, leftValue: IValue, expectFalse: boolean): IValue {
        leftValue = leftValue.cast(Types.bool);
        return new Evaluator(
            owner
            , Types.bool
            , null
            , hash({ isFalse: leftValue.hash, expectFalse })
            , leftValue
            , expectFalse ? ((raw, t) => {
                const left = leftValue.get(raw, t);
                return left === false; // never returns null
            }) : ((raw, t) => {
                const left = leftValue.get(raw, t);
                return !(left === false); //  never returns null
            }));
    },
    negate(value: IValue): IValue {
        if (value.type === Types.bool) {
            return (value as Evaluator)
                .setConversion(x => !x, x => ({ not: x }));
        }
        if (!isNumeric(value.type)) {
            throw new QueryError('Can only apply "-" unary operator to numeric types');
        }
        return (value as Evaluator)
            .setConversion(x => -x, x => ({ neg: x }));
    },
    array(owner: _ISchema, values: IValue[]): IValue {
        return arrayOrList(owner, values, false);
    },
    list(owner: _ISchema, values: IValue[]): IValue {
        return arrayOrList(owner, values, true);
    }
} as const;


function arrayOrList(owner: _ISchema, values: IValue[], list: boolean) {
    const type = values.reduce((t, v) => {
        if (v.canCast(t)) {
            return t;
        }
        if (!t.canCast(v.type)) {
            throw new CastError(t.primary, v.type.primary);
        }
        return v.type;
    }, Types.null);
    // const sel = values.find(x => !!x.selection)?.selection;
    const converted = values.map(x => x.cast(type));
    return new Evaluator(
        owner
        , list ? type.asList() : type.asArray()
        , null
        , hash(converted.map(x => x.hash))
        , converted
        , (raw, t) => {
            const arr = values.map(x => x.get(raw, t));
            return arr;
        });
}