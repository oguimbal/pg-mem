import { IValue, _IIndex, _ISelection, _IType, TR } from './interfaces-private';
import { DataType, CastError, IType, QueryError, NotSupported } from './interfaces';
import moment from 'moment';
import hash from 'object-hash';
import { deepEqual, deepCompare, nullIsh } from './utils';
import { Evaluator, Value } from './valuetypes';
import { DataTypeDef } from './parser/syntax/ast';
import { parseArrayLiteral } from './parser/parser';

abstract class TypeBase<TRaw = any> implements _IType<TRaw> {

    abstract primary: DataType;
    abstract regTypeName: string;
    /** Can be casted to */
    doCanCast?(to: _IType<TRaw>): boolean;

    /**
     * @see this.prefer() doc
      */
    doPrefer?(type: _IType<TRaw>): _IType | null;

    /**
     * @see this.canConvertImplicit() doc
     */
    doCanConvertImplicit?(to: _IType<TRaw>): boolean;

    /** Perform conversion */
    doCast?(value: Evaluator<TRaw>, to: _IType<TRaw>): Evaluator<any>;

    doEquals(a: any, b: any): boolean {
        return a === b;
    }

    doGt(a: any, b: any): boolean {
        return a > b;
    }

    doLt(a: any, b: any): boolean {
        return a < b;
    }
    toString(): string {
        throw new Error('Method not implemented.');
    }

    equals(a: TRaw, b: TRaw): boolean {
        if (a === null || b === null) {
            return null;
        }
        return this.doEquals(a, b);
    }

    gt(a: TRaw, b: TRaw): boolean {
        if (a === null || b === null) {
            return null;
        }
        return this.doGt(a, b);
    }
    lt(a: TRaw, b: TRaw): boolean {
        if (a === null || b === null) {
            return null;
        }
        return this.doLt(a, b);
    }

    ge(a: TRaw, b: TRaw): boolean {
        return this.gt(a, b) || this.equals(a, b);
    }

    le(a: TRaw, b: TRaw): boolean {
        return this.lt(a, b) || this.equals(a, b);
    }

    /**
     * When performing 'a+b', will be given 'b' type,
     * this returns the prefered resulting type, or null if they are not compatible
      */
    prefer(type: DataType | _IType<TRaw>): _IType | null | undefined {
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
        return to.doPrefer && to.doPrefer(this);
    }

    /**
     * Can constant literals be converted implicitely
     * (without a cast... i.e. you can use both values as different values of a case expression, for instance)
     **/
    canConvertImplicit(_to: DataType | _IType<TRaw>): boolean {
        const to = makeType(_to);
        if (to === this) {
            return true;
        }
        return this.doCanConvertImplicit && this.doCanConvertImplicit(to);
    }

    /** Can be explicitely casted to */
    canConvert(_to: DataType | _IType<TRaw>): boolean {
        const to = makeType(_to);
        if (to === this) {
            return true;
        }
        return this.doCanCast && this.doCanCast(to);
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
        const ev = new Evaluator(this, null, null, null, null, () => current, {
            unpure: true,
        });
        const converted = this.convert(ev, _to);
        return (source: TRaw) => {
            current = source;
            return converted.get()
        };
    }
}


class RegType extends TypeBase<_IType> {

    get regTypeName(): string {
        return 'regtype';
    }

    get primary(): DataType {
        return DataType.regtype;
    }

    doCanCast(_to: _IType): boolean {
        switch (_to.primary) {
            case DataType.text:
            case DataType.int:
                return true;
        }
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        switch (to.primary) {
            case DataType.text:
                return a
                    .setType(Types.text())
                    .setConversion((raw: string) => {
                        return raw;
                    }
                        , s => `(${s})::TEXT`
                        , toText => ({ toText }))
            case DataType.int:
                return a
                    .setType(Types.text())
                    .setConversion((raw: string) => {
                        const got = parseRegType(raw);
                        return typeIndexes[got.primary]
                    }
                        , s => `(${s})::TEXT`
                        , toText => ({ toText }))
        }
    }

    doEquals(a: _IType, b: _IType): boolean {
        return a.primary === b.primary;
    }

    doGt(a: _IType, b: _IType): boolean {
        return a.primary > b.primary;
    }

    doLt(a: _IType, b: _IType): boolean {
        return a.primary < b.primary;
    }
}


class JSONBType extends TypeBase<any> {

    get regTypeName(): string {
        return 'jsonb';
    }

    constructor(readonly primary: DataType) {
        super();
    }

    doCanCast(_to: _IType): boolean {
        switch (_to.primary) {
            case DataType.text:
            case DataType.json:
            case DataType.jsonb:
                return true;
        }
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        if (to.primary === DataType.json) {
            return a
                .setType(Types.text())
                .setConversion(json => JSON.stringify(json)
                    , s => `(${s})::JSONB`
                    , toJsonB => ({ toJsonB }))
                .convert(to) as Evaluator; // <== might need truncation
        }

        // json
        return a.setType(to);
    }

    doEquals(a: any, b: any): boolean {
        return deepEqual(a, b, false);
    }

    doGt(a: any, b: any): boolean {
        return deepCompare(a, b) > 0;
    }

    doLt(a: any, b: any): boolean {
        return deepCompare(a, b) < 0;
    }
}

class TimestampType extends TypeBase<Date> {

    get regTypeName(): string {
        return 'timestamp without time zone';
    }

    constructor(readonly primary: DataType) {
        super();
    }

    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.timestamp:
            case DataType.date:
                return true;
        }
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.timestamp:
                return value;
            case DataType.date:
                value
                    .setConversion(raw => moment(raw).startOf('day').toDate()
                        , sql => `(${sql})::date`
                        , toDate => ({ toDate }));
        }
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

    get regTypeName(): string {
        return null;
    }

    get primary(): DataType {
        return DataType.null;
    }

    doCast(value: Evaluator<any>, to: _IType): Evaluator<any> {
        return new Evaluator(to, null, 'null', 'null', null, null);
    }

    doCanCast(to: _IType): boolean {
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

    doPrefer(type) {
        return type; // always prefer notnull types
    }
}

const integers = new Set([DataType.int, DataType.long]);
const numbers = new Set([DataType.int, DataType.long, DataType.decimal, DataType.float]);

export function isNumeric(t: IType) {
    return numbers.has(t.primary);
}
export function isInteger(t: IType) {
    return integers.has(t.primary);
}

class NumberType extends TypeBase<number> {

    get regTypeName(): string {
        switch (this.primary) {
            case DataType.int:
            case DataType.long:
                return 'integer';
            case DataType.float:
            case DataType.decimal:
                return 'double precision';
            default:
                throw new NotSupported('Retype name of ' + this.primary);
        }
    }

    constructor(readonly primary: DataType) {
        super();
    }

    doCanConvertImplicit(to: _IType) {
        switch (to.primary) {
            case DataType.int:
            case DataType.long:
            case DataType.float:
            case DataType.decimal:
            case DataType.regtype:
                return true;
            default:
                return false;
        }
    }

    doPrefer(type: _IType): _IType | undefined {
        switch (type.primary) {
            case DataType.int:
            case DataType.long:
                return this;
            case DataType.float:
            case DataType.decimal:
                return type;
        }
    }

    canConvert(to: _IType) {
        switch (to.primary) {
            case DataType.int:
            case DataType.long:
            case DataType.float:
            case DataType.decimal:
            case DataType.regtype:
                return true;
            default:
                return false;
        }
    }
    doCast(value: Evaluator<any>, to: _IType): Evaluator<any> {
        if (!integers.has(value.type.primary) && integers.has(to.primary)) {
            return new Evaluator(to
                , value.id
                , value.sql
                , value.hash
                , value
                , (raw, t) => {
                    const got = value.get(raw, t);
                    return typeof got === 'number'
                        ? Math.round(got)
                        : got;
                }
            );
        }
        if (to.primary === DataType.regtype) {
            return value
                .setType(Types.regtype)
                .setConversion((int: number) => {
                    const got = Types[DataType[allTypes[int]]] as _IType;
                    if (!got) {
                        throw new CastError(DataType.int, DataType.regtype);
                    }
                    return got.regTypeName;
                }
                    , sql => `(${sql})::regtype`
                    , intToRegType => ({ intToRegType }));
        }
        return new Evaluator(to
            , value.id
            , value.sql
            , value.hash
            , value
            , value.val
        );
    }
}

class TextType extends TypeBase<string> {

    get regTypeName(): string {
        return this.len ? 'character varying' : 'text';
    }

    get primary(): DataType {
        return DataType.text;
    }

    constructor(readonly len: number | null) {
        super();
    }

    doPrefer(to: _IType) {
        if (this.canConvert(to)) {
            return to;
        }
        return null;
    }

    doCanConvertImplicit(to: _IType): boolean {
        // text is implicitely convertible to dates
        switch (to.primary) {
            case DataType.text:
            case DataType.bool:
                return true;
        }
    }

    doCanCast(to: _IType): boolean {
        switch (to.primary) {
            case DataType.timestamp:
            case DataType.date:
                return true;
            case DataType.text:
                return true;
            case DataType.jsonb:
            case DataType.json:
                return true;
            case DataType.regtype:
                return true;
            case DataType.bool:
                return true;
            case DataType.array:
                return this.canConvert((to as ArrayType).of);
        }
        if (numbers.has(to.primary)) {
            return true;
        }
    }

    doCast(value: Evaluator<string>, to: _IType) {
        switch (to.primary) {
            case DataType.timestamp:
                return value
                    .setConversion(str => {
                        const conv = moment.utc(str);
                        if (!conv.isValid()) {
                            throw new QueryError(`Invalid timestamp format: ` + str);
                        }
                        return conv.toDate()
                    }
                        , sql => `(${sql})::timestamp`
                        , toTs => ({ toTs }));
            case DataType.date:
                return value
                    .setConversion(str => {
                        const conv = moment.utc(str);
                        if (!conv.isValid()) {
                            throw new QueryError(`Invalid timestamp format: ` + str);
                        }
                        return conv.startOf('day').toDate();
                    }
                        , sql => `(${sql})::date`
                        , toDate => ({ toDate }));
            case DataType.bool:
                return value
                    .setConversion(rawStr => {
                        if (nullIsh(rawStr)) {
                            return null;
                        }
                        const str = (rawStr as string).toLowerCase();
                        if ('true'.startsWith(str)) {
                            return true;
                        } else if ('false'.startsWith(str)) {
                            return false;
                        }
                        throw new CastError(DataType.text, DataType.bool, 'string ' + rawStr);
                    }
                        , sql => `(${sql})::boolean`
                        , toBool => ({ toBool }));
            case DataType.json:
            case DataType.jsonb:
                return value
                    .setConversion(raw => JSON.parse(raw)
                        , sql => `(${sql})::jsonb`
                        , toJsonb => ({ toJsonb }));
            case DataType.text:
                const fromStr = to as TextType;
                const toStr = to as TextType;
                if (toStr.len === null || fromStr.len < toStr.len) {
                    // no need to truncate
                    return value;
                }
                return value
                    .setConversion(str => {
                        if (str?.length > toStr.len) {
                            throw new QueryError(`value too long for type character varying(${toStr.len})`);
                        }
                        return str;
                    }
                        , sql => `TRUNCATE(${sql}, ${toStr.len})`
                        , truncate => ({ truncate, len: toStr.len }));
            case DataType.regtype:
                return value
                    .setType(Types.regtype)
                    .setConversion((str: string) => {
                        let repl = str.replace(/["\s]+/g, '');
                        if (repl.startsWith('pg_catalog.')) {
                            repl = repl.substr('pg_catalog.'.length);
                        }
                        return parseRegType(repl).regTypeName;
                    }
                        , sql => `(${sql})::regtype`
                        , strToRegType => ({ strToRegType }));
            case DataType.array:
                return value
                    .setType(to)
                    .setConversion((str: string) => {
                        const array = parseArrayLiteral(str);
                        (to as ArrayType).convertLiteral(array);
                        return array;
                    }
                        , sql => `(${sql})::${to.regTypeName}`
                        , parseArray => ({ parseArray }));

        }
        if (numbers.has(to.primary)) {
            const isInt = integers.has(to.primary);
            return value
                .setConversion(str => {
                    const val = Number.parseFloat(str);
                    if (!Number.isFinite(val)) {
                        throw new QueryError(`invalid input syntax for ${to.primary}: ${str}`);
                    }
                    if (isInt && Math.floor(val) !== val) {
                        throw new QueryError(`invalid input syntax for ${to.primary}: ${str}`)
                    }
                    return val;
                }
                    , sql => `(${sql})::${to.primary}`
                    , castNum => ({ castNum, to: to.primary }));
        }
    }
}

class BoolType extends TypeBase<boolean> {

    get regTypeName(): string {
        return 'boolean';
    }

    get primary(): DataType {
        return DataType.bool;
    }
}

export class ArrayType extends TypeBase<any[]> {
    get primary(): DataType {
        return DataType.array;
    }

    get regTypeName(): string {
        return this.of.regTypeName + '[]';
    }


    constructor(readonly of: _IType) {
        super();
    }

    doCanCast(to: _IType) {
        return to instanceof ArrayType
            && to.canConvert(this.of);
    }

    doCast(value: IValue, _to: _IType) {
        const to = _to as ArrayType;
        const valueType = value.type as ArrayType;
        return new Evaluator(to
            , value.id
            , value.sql
            , value.hash
            , value
            , (raw, t) => {
                const arr = value.get(raw, t) as any[];
                return arr.map(x => Value.constant(x, valueType.of).convert(to.of).get(raw, t));
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

    convertLiteral(elts: any) {
        if (elts === null || elts === undefined) {
            return;
        }
        if (!Array.isArray(elts)) {
            throw new QueryError('Array depth mismatch: was expecting an array item.');
        }
        if (this.of instanceof ArrayType) {
            for (let i = 0; i < elts.length; i++) {
                this.of.convertLiteral(elts[i]);
            }
        } else {
            for (let i = 0; i < elts.length; i++) {
                if (Array.isArray(elts[i])) {
                    throw new QueryError('Array depth mismatch: was not expecting an array item.');
                }
                elts[i] = Value.text(elts[i])
                    .convert(this.of)
                    .get();
            }
        }
        return elts;
    }
}

export function makeType(to: DataType | _IType<any>): _IType<any> {
    if (typeof to === 'string') {
        if (to === DataType.text) {
            return Types.text();
        }
        if (!Types[to]) {
            throw new Error('Unsupported raw type: ' + to);
        }
        return Types[to];
    }
    return to;
}


// type Ctors = {
//     [key in DataType]?: _IType;
// };
export const Types = { // : Ctors
    [DataType.bool]: new BoolType() as _IType,
    [DataType.text]: (len = null) => makeText(len) as _IType,
    [DataType.timestamp]: new TimestampType(DataType.timestamp) as _IType,
    [DataType.date]: new TimestampType(DataType.date) as _IType,
    [DataType.jsonb]: new JSONBType(DataType.jsonb) as _IType,
    [DataType.regtype]: new RegType() as _IType,
    [DataType.json]: new JSONBType(DataType.json) as _IType,
    [DataType.null]: new NullType() as _IType,
    [DataType.float]: new NumberType(DataType.float) as _IType,
    [DataType.int]: new NumberType(DataType.int) as _IType,
    [DataType.long]: new NumberType(DataType.long) as _IType,
}


const typeIndexes = {};
const allTypes = Object.keys(DataType);
for (let i = 0; i < allTypes.length; i++) {
    typeIndexes[DataType[allTypes[i]]] = i + 1;
}

const texts = new Map<number, _IType>();
export function makeText(len: number = null) {
    len = len ?? null;
    let got = texts.get(len);
    if (!got) {
        texts.set(len, got = new TextType(len));
    }
    return got;
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


export function parseRegType(native: string): _IType {
    if (/\[\]$/.test(native)) {
        const inner = parseRegType(native.substr(0, native.length - 2));
        return makeArray(inner);
    }
    return fromNative({ type: native });
}

export function fromNative(native: DataTypeDef): _IType {
    switch (native.type) {
        case 'text':
        case 'varchar':
            return Types.text(native.length);
        case 'int':
        case 'integer':
        case 'serial':
            return Types.int;
        case 'decimal':
        case 'float':
            return Types.float;
        case 'timestamp':
            return Types.timestamp;
        case 'date':
            return Types.date;
        case 'json':
            return Types.json;
        case 'jsonb':
            return Types.jsonb;
        case 'regtype':
            return Types.regtype;
        case 'array':
            return makeArray(fromNative(native.arrayOf));
        default:
            throw new NotSupported('Type ' + JSON.stringify(native.type));
    }
}


/** Finds a common type by implicit conversion */
export function reconciliateTypes(values: IValue[]): _IType {
    // let typeConstraints = values
    //     .filter(x => !x.isConstantLiteral);

    // // if there are non constant literals, constant literals must match them.
    // if (!typeConstraints.length) {
    //     typeConstraints = values;
    // }

    // find the matching type among non constants
    const foundType = values
        .reduce((final, c) => {
            const pref = final.prefer(c.type);
            if (!pref) {
                throw new CastError(c.type.primary, final.primary, c.sql);
            }
            return pref;
        }, Types.null);

    // check that all constant literals are matching this.
    for (const x of values) {
        if (!x.isConstantLiteral && !x.type.canConvertImplicit(foundType)) {
            throw new CastError(x.type.primary, foundType.primary);
        }
    }

    return foundType;
}