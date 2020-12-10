import { IValue, _IIndex, _ISelection, _IType, TR, RegClass, RegType } from './interfaces-private.ts';
import { DataType, CastError, IType, QueryError, NotSupported, nil } from './interfaces.ts';
import moment from 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts';
import { deepEqual, deepCompare, nullIsh, getContext } from './utils.ts';
import { Evaluator, Value } from './valuetypes.ts';
import { DataTypeDef, parse, QName } from 'https://deno.land/x/pgsql_ast_parser@1.3.7/mod.ts';
import { parseArrayLiteral } from 'https://deno.land/x/pgsql_ast_parser@1.3.7/mod.ts';
import { bufCompare, bufFromString, bufToString, TBuffer } from './buffer-deno.ts';

abstract class TypeBase<TRaw = any> implements _IType<TRaw> {

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


class RegTypeImpl extends TypeBase<RegType> {

    get regTypeName(): string {
        return 'regtype';
    }

    get primary(): DataType {
        return DataType.regtype;
    }

    doCanCast(_to: _IType): boolean | nil {
        switch (_to.primary) {
            case DataType.text:
            case DataType.int:
                return true;
        }
        return null;
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
                        , s => `(${s})::INT`
                        , toText => ({ toText }))
        }
        throw new Error('failed to cast');
    }
}

class RegClassImpl extends TypeBase<RegClass> {

    get regTypeName(): string {
        return 'regclass';
    }

    get primary(): DataType {
        return DataType.regclass;
    }

    doCanCast(_to: _IType): boolean | nil {
        switch (_to.primary) {
            case DataType.text:
            case DataType.int:
                return true;
        }
        return null;
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        switch (to.primary) {
            case DataType.text:
                return a
                    .setType(Types.text())
                    .setConversion((raw: RegClass) => {
                        return raw?.toString();
                    }
                        , s => `(${s})::TEXT`
                        , toText => ({ toText }))
            case DataType.int:
                return a
                    .setType(Types.text())
                    .setConversion((raw: RegClass) => {

                        // === regclass -> int

                        const cls = parseRegClass(raw);
                        const { schema } = getContext();

                        // if its a number, then try to get it.
                        if (typeof cls === 'number') {
                            return schema.getObjectByRegOrName(cls)
                                ?.reg.classId
                                ?? cls;
                        }

                        // get the object or throw
                        return schema.getObjectByRegOrName(raw)
                            .reg.classId;
                    }
                        , s => `(${s})::INT`
                        , toText => ({ toText }))
        }
        throw new Error('failed to cast');
    }
}

class JSONBType extends TypeBase<any> {

    get regTypeName(): string {
        return 'jsonb';
    }

    constructor(readonly primary: DataType) {
        super();
    }

    doCanCast(_to: _IType): boolean | nil {
        switch (_to.primary) {
            case DataType.text:
            case DataType.json:
            case DataType.jsonb:
                return true;
        }
        return null;
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

class UUIDtype extends TypeBase<Date> {

    get regTypeName(): string {
        return 'uuid';
    }

    get primary(): DataType {
        return DataType.uuid;
    }

    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return true;
        }
        return null;
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return value;
        }
        throw new Error('Unexpected cast error');
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
            case DataType.time:
                return true;
        }
        return null;
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.timestamp:
                return value;
            case DataType.date:
                return value
                    .setConversion(raw => moment(raw).startOf('day').toDate()
                        , sql => `(${sql})::date`
                        , toDate => ({ toDate }));
            case DataType.time:
                return value
                    .setConversion(raw => moment(raw).format('HH:mm:ss') + '.000000'
                        , sql => `(${sql})::date`
                        , toDate => ({ toDate }));
        }
        throw new Error('Unexpected cast error');
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

    get regTypeName(): string | null {
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

    doPrefer(type: _IType) {
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
            case DataType.regclass:
                return true;
            default:
                return false;
        }
    }

    doPrefer(type: _IType): _IType | null {
        switch (type.primary) {
            case DataType.int:
            case DataType.long:
                return this;
            case DataType.float:
            case DataType.decimal:
                return type;
        }
        return null;
    }

    canConvert(to: _IType) {
        switch (to.primary) {
            case DataType.int:
            case DataType.long:
            case DataType.float:
            case DataType.decimal:
            case DataType.regtype:
            case DataType.regclass:
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
                    const got = makeType((DataType as any)[allTypes[int]]);
                    if (!got) {
                        throw new CastError(DataType.int, DataType.regtype);
                    }
                    return got.regTypeName;
                }
                    , sql => `(${sql})::regtype`
                    , intToRegType => ({ intToRegType }));
        }
        if (to.primary === DataType.regclass) {
            return value
                .setType(Types.regclass)
                .setConversion((int: number) => {
                    // === int -> regclass
                    const { schema } = getContext();
                    const obj = schema.getObjectByRegOrName(int, { nullIfNotFound: true });
                    return obj?.reg.classId ?? int;
                }
                    , sql => `(${sql})::regclass`
                    , intToRegClass => ({ intToRegClass }));
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

class TimeType extends TypeBase<string> {
    get regTypeName() {
        return 'time';
    }

    get primary(): DataType {
        return DataType.time;
    }


    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return true;
        }
        return null;
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return value
                    .setType(Types.text())
        }
        throw new Error('Unexpected cast error');
    }
}

class ByteArrayType extends TypeBase<TBuffer> {
    get regTypeName() {
        return 'bytea';
    }

    get primary(): DataType {
        return DataType.bytea;
    }


    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return true;
        }
        return null;
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return value
                    .setConversion(raw => bufToString(raw)
                        , sql => `(${sql})::text`
                        , toStr => ({ toStr }));
        }
        throw new Error('Unexpected cast error');
    }

    doEquals(a: TBuffer, b: TBuffer): boolean {
        return bufCompare(a, b) === 0;
    }

    doGt(a: TBuffer, b: TBuffer): boolean {
        return bufCompare(a, b) > 0;
    }

    doLt(a: TBuffer, b: TBuffer): boolean {
        return bufCompare(a, b) < 0;
    }
}


class TextType extends TypeBase<string> {

    get regTypeName(): string {
        if (this.citext) {
            return 'citext';
        }
        return this.len ? 'character varying' : 'text';
    }

    get primary(): DataType {
        return this.citext
            ? DataType.citext
            : DataType.text;
    }

    constructor(readonly len: number | null, private citext?: boolean) {
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
            case DataType.uuid:
            case DataType.bytea:
                return true;
        }
        return false;
    }

    doCanCast(to: _IType): boolean | nil {
        switch (to.primary) {
            case DataType.text:
            case DataType.citext:
                return true;
            case DataType.timestamp:
            case DataType.date:
            case DataType.time:
                return true;
            case DataType.text:
            case DataType.uuid:
                return true;
            case DataType.jsonb:
            case DataType.json:
                return true;
            case DataType.regtype:
            case DataType.regclass:
                return true;
            case DataType.bool:
                return true;
            case DataType.array:
                return this.canConvert((to as ArrayType).of);
            case DataType.bytea:
                return true;
        }
        if (numbers.has(to.primary)) {
            return true;
        }
        return undefined;
    }

    doCast(value: Evaluator<string>, to: _IType) {
        switch (to.primary) {
            case DataType.citext:
                return value.setType(to);
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
            case DataType.time:
                return value
                    .setConversion(str => {
                        const conv = moment.utc(str, 'HH:mm:ss');
                        if (!conv.isValid()) {
                            throw new QueryError(`Invalid time format: ` + str);
                        }
                        return conv.format('HH:mm:ss.000000');
                    }
                        , sql => `(${sql})::time`
                        , toTime => ({ toTime }));
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
                        if ('yes'.startsWith(str)) {
                            return true;
                        } else if ('no'.startsWith(str)) {
                            return false;
                        }
                        throw new CastError(DataType.text, DataType.bool, 'string ' + rawStr);
                    }
                        , sql => `(${sql})::boolean`
                        , toBool => ({ toBool }));
            case DataType.uuid:
                return value
                    .setConversion((_rawStr: string) => {
                        let rawStr = _rawStr;
                        if (nullIsh(rawStr)) {
                            return null;
                        }
                        // check schema
                        if (rawStr[0] === '{') {
                            if (rawStr[rawStr.length - 1] !== '}') {
                                throw new CastError(DataType.text, DataType.uuid, 'string: ' + JSON.stringify(_rawStr));
                            }
                            rawStr = rawStr.substr(1, rawStr.length - 2);
                        }
                        rawStr = rawStr.toLowerCase();
                        const [full, a, b, c, d, e] = /^([0-9a-f]{8})-?([0-9a-f]{4})-?([0-9a-f]{4})-?([0-9a-f]{4})-?([0-9a-f]{12})$/.exec(rawStr) ?? [];
                        if (!full) {
                            throw new CastError(DataType.text, DataType.uuid, 'string: ' + JSON.stringify(_rawStr));
                        }
                        return `${a}-${b}-${c}-${d}-${e}`;
                    }
                        , sql => `(${sql})::uuid`
                        , toUuid => ({ toUuid }));
            case DataType.json:
            case DataType.jsonb:
                return value
                    .setConversion(raw => JSON.parse(raw)
                        , sql => `(${sql})::jsonb`
                        , toJsonb => ({ toJsonb }));
            case DataType.text:
                const fromStr = to as TextType;
                const toStr = to as TextType;
                if (toStr.len === null || (fromStr.len ?? -1) < toStr.len) {
                    // no need to truncate
                    return value;
                }
                return value
                    .setConversion(str => {
                        if (str?.length > toStr.len!) {
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
            case DataType.regclass:
                return value
                    .setType(Types.regclass)
                    .setConversion((str: string) => {
                        // === text -> regclass

                        const cls = parseRegClass(str);
                        const { schema } = getContext();

                        // if its a number, then try to get it.
                        if (typeof cls === 'number') {
                            return schema.getObjectByRegOrName(cls)
                                ?.name
                                ?? cls;
                        }

                        // else, get or throw.
                        return schema.getObject(cls)
                            .name;
                    }
                        , sql => `(${sql})::regclass`
                        , strToRegClass => ({ strToRegClass }));
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
            case DataType.bytea:
                return value
                    .setConversion(str => {
                        return bufFromString(str);
                    }
                        , sql => `(${sql})::bytea`
                        , toBytea => ({ toBytea }));

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
        return undefined;
    }

    doEquals(a: string, b: string) {
        if (this.citext) {
            return a.localeCompare(b, undefined, { sensitivity: 'accent' }) === 0;
        }

        return super.doEquals(a, b);
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
            && to.of.canConvert(this.of);
    }

    doCast(value: IValue, _to: _IType) {
        const to = _to as ArrayType;
        const valueType = value.type as ArrayType;
        return new Evaluator(to
            , value.id
            , value.sql
            , value.hash!
            , value
            , (raw, t) => {
                const arr = value.get(raw, t) as any[];
                return arr.map(x => Value.constant(valueType.of, x).convert(to.of).get(raw, t));
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

export function makeType(to: DataType | IType | _IType<any>): _IType<any> {
    if (typeof to === 'string') {
        const ret = Types[to as keyof typeof Types];
        if (!ret) {
            throw new Error('Unsupported raw type: ' + to);
        }
        return typeof ret === 'function'
            ? ret()
            : ret;
    }
    return to as _IType;
}


// type Ctors = {
//     [key in DataType]?: _IType;
// };
export const Types = { // : Ctors
    [DataType.bool]: new BoolType() as _IType,
    [DataType.text]: (len: number | nil = null) => makeText(len) as _IType,
    [DataType.citext]: new TextType(null, true),
    [DataType.timestamp]: new TimestampType(DataType.timestamp) as _IType,
    [DataType.uuid]: new UUIDtype() as _IType,
    [DataType.date]: new TimestampType(DataType.date) as _IType,
    [DataType.time]: new TimeType() as _IType,
    [DataType.jsonb]: new JSONBType(DataType.jsonb) as _IType,
    [DataType.regtype]: new RegTypeImpl() as _IType,
    [DataType.regclass]: new RegClassImpl() as _IType,
    [DataType.json]: new JSONBType(DataType.json) as _IType,
    [DataType.null]: new NullType() as _IType,
    [DataType.float]: new NumberType(DataType.float) as _IType,
    [DataType.int]: new NumberType(DataType.int) as _IType,
    [DataType.long]: new NumberType(DataType.long) as _IType,
    [DataType.bytea]: new ByteArrayType() as _IType,
}


const typeIndexes: { [key: string]: number } = {};
const allTypes = Object.keys(DataType);
for (let i = 0; i < allTypes.length; i++) {
    typeIndexes[DataType[allTypes[i] as keyof typeof Types]] = i + 1;
}

const texts = new Map<number | null, _IType>();
export function makeText(len: number | nil = null) {
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



export function parseRegClass(_reg: RegClass): QName | number {
    let reg = _reg;
    if (typeof reg === 'string' && /^\d+$/.test(reg)) {
        reg = parseInt(reg);
    }
    if (typeof reg === 'number') {
        return reg;
    }
    // todo remove casts after next pgsql-ast-parser release
    try {
        const ret = parse(reg, 'qualified_name' as any) as QName;
        return ret;
    } catch (e) {
        return { name: reg };
    }
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
        case 'char':
        case 'character':
        case 'character varying':
            return Types.text(native.length);
        case 'citext':
            return Types.citext;
        case 'uuid':
            return Types.uuid;
        case 'int':
        case 'integer':
        case 'serial':
        case 'bigserial':
        case 'smallserial':
        case 'smallint':
        case 'bigint':
        case 'oid':
            return Types.int;
        case 'decimal':
        case 'float':
        case 'double precision':
        case 'numeric':
        case 'real':
        case 'money':
            return Types.float;
        case 'timestamp':
            return Types.timestamp;
        case 'date':
        case 'timestamp':
        case 'timestamp with time zone':
        case 'timestamp without time zone':
            return Types.date;
        case 'json':
            return Types.json;
        case 'jsonb':
            return Types.jsonb;
        case 'regtype':
            return Types.regtype;
        case 'regclass':
            return Types.regclass;
        case 'array':
            return makeArray(fromNative(native.arrayOf!));
        case 'boolean':
        case 'bool':
            return Types.bool;
        case 'bytea':
            return Types.bytea;
        case 'time':
        case 'time with time zone':
        case 'time without time zone':
            return Types.time;
        default:
            throw new NotSupported('Data type ' + JSON.stringify(native.type));
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
                throw new CastError(c.type.primary, final.primary, c.sql ?? undefined);
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