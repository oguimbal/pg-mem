import { IValue, _IIndex, _ISelection, _IType, _ISchema } from '../interfaces-private';
import { DataType, CastError, IType, QueryError, nil } from '../interfaces';
import { nullIsh } from '../utils';
import { Evaluator, Value } from '../evaluator';
import { parseArrayLiteral } from 'pgsql-ast-parser';
import { parseGeometricLiteral } from 'pgsql-ast-parser';
import { bufCompare, bufFromString, bufToString, TBuffer } from '../misc/buffer-node';
import { TypeBase } from './datatype-base';
import { BoxType, CircleType, LineType, LsegType, PathType, PointType, PolygonType } from './datatypes-geometric';
import { IntervalType } from './t-interval';
import { TimeType } from './t-time';
import { TimestampType } from './t-timestamp';
import { JSONBType } from './t-jsonb';
import { RegTypeImpl } from './t-regtype';
import { RegClassImpl } from './t-regclass';
import { RecordType } from './t-record';
import { INetType } from './t-inet';
import { buildCtx } from '../parser/context';


class UUIDtype extends TypeBase<string> {


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



class NullType extends TypeBase<null> {

    get primary(): DataType {
        return DataType.null;
    }

    doCast(value: Evaluator<any>, to: _IType): Evaluator<any> {
        return new Evaluator(to, null, 'null', null, null);
    }

    doCanCast(to: _IType): boolean {
        return true;
    }

    doCanConvertImplicit() {
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

export class DefaultType extends NullType {
}

export const integers: ReadonlySet<DataType> = new Set([DataType.integer, DataType.bigint]);
export const floats: ReadonlySet<DataType> = new Set([DataType.decimal, DataType.float]);
export const numbers: ReadonlySet<DataType> = new Set([...integers, ...floats]);
export const numberPriorities = [DataType.integer, DataType.bigint, DataType.decimal, DataType.float]
    .reduce<Record<DataType, number>>((a, x, i) => ({
        ...a,
        [x]: i
    }), {} as Record<DataType, number>);

export function isNumeric(t: DataType | IType) {
    const type = typeof t === 'string' ? t : t.primary;
    return numbers.has(type);
}
export function isInteger(t: DataType | IType) {
    const type = typeof t === 'string' ? t : t.primary;
    return integers.has(type);
}

class NumberType extends TypeBase<number> {

    constructor(readonly primary: DataType) {
        super();
    }

    doCanConvertImplicit(to: _IType) {
        switch (to.primary) {
            case DataType.integer:
            case DataType.bigint:
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
            case DataType.integer:
            case DataType.bigint:
                return this;
            case DataType.float:
            case DataType.decimal:
                return type;
        }
        return null;
    }

    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.integer:
            case DataType.bigint:
            case DataType.float:
            case DataType.decimal:
            case DataType.regtype:
            case DataType.regclass:
                return true;
            case DataType.text:
                return true;
            default:
                return false;
        }
    }
    doCast(value: Evaluator<any>, to: _IType): Evaluator<any> {
        if (!integers.has(value.type.primary) && integers.has(to.primary)) {
            return new Evaluator(
                to
                , value.id
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
        const { schema } = buildCtx();
        switch (to.primary) {
            case DataType.regtype:
                return value
                    .setType(Types.regtype)
                    .setConversion((int: number) => {
                        const got = schema.getType(int, { nullIfNotFound: true });
                        if (!got) {
                            throw new CastError(DataType.integer, DataType.regtype);
                        }
                        return got.name;
                    }
                        , intToRegType => ({ intToRegType }));
            case DataType.regclass:
                return value
                    .setType(Types.regclass)
                    .setConversion((int: number) => {
                        // === int -> regclass
                        const obj = schema.getObjectByRegOrName(int, { nullIfNotFound: true });
                        return obj?.reg.classId ?? int;
                    }
                        , intToRegClass => ({ intToRegClass }));
            case DataType.text:
                return value
                    .setType(to)
                    .setConversion((int: number) => int.toString()
                        , toTxt => ({ toTxt }));
        }
        return value.setType(to);
    }
}



class ByteArrayType extends TypeBase<TBuffer> {

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

    get name(): string {
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
        if (to instanceof TextType) {
            // returns the broader type
            if (!to.len) {
                return to;
            }
            if (!this.len) {
                return this;
            }
            return to.len > this.len ? to : this;
        }
        if (this.canCast(to)) {
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
            case DataType.text:
            case DataType.uuid:
                return true;
            case DataType.bool:
                return true;
            case DataType.array:
                return this.canCast((to as ArrayType).of);
            case DataType.bytea:
                return true;
        }
        if (numbers.has(to.primary)) {
            return true;
        }
        if (isGeometric(to.primary)) {
            return true;
        }
        return undefined;
    }

    doCast(value: Evaluator<string>, to: _IType) {
        switch (to.primary) {
            case DataType.citext:
                return value.setType(to);
            case DataType.bool:
                return value
                    .setConversion(rawStr => {
                        if (nullIsh(rawStr)) {
                            return null;
                        }
                        if (rawStr === '0') {
                            return false;
                        } else if (rawStr === '1') {
                            return true;
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
                        , toUuid => ({ toUuid }));
            case DataType.text:
                const fromStr = to as TextType;
                const toStr = to as TextType;
                if (toStr.len === null || (fromStr.len ?? -1) < toStr.len) {
                    // no need to truncate
                    return value;
                }
                return value
                    .setConversion(str => {
                        if (str && str.length > toStr.len!) {
                            throw new QueryError(`value too long for type character varying(${toStr.len})`);
                        }
                        return str;
                    }
                        , truncate => ({ truncate, len: toStr.len }));

            case DataType.array:
                return value
                    .setType(to)
                    .setConversion((str: string) => {
                        const array = parseArrayLiteral(str);
                        (to as ArrayType).convertLiteral(array);
                        return array;
                    }
                        , parseArray => ({ parseArray }));
            case DataType.bytea:
                return value
                    .setConversion(str => {
                        return bufFromString(str);
                    }
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
                    , castNum => ({ castNum, to: to.primary }));
        }
        if (isGeometric(to.primary)) {
            return value
                .setConversion(str => {
                    const ret = parseGeometricLiteral(str, to.primary as any);
                    return ret;
                }
                    , castGeo => ({ castGeo, to: to.primary }));
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
    get primary(): DataType {
        return DataType.bool;
    }

    doCanCast(to: _IType): boolean | nil {
        switch (to.primary) {
            case DataType.text:
            case DataType.citext:
            case DataType.bool:
            case DataType.integer:
                return true;
        }
        return false;
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.text:
            case DataType.citext:
                return new Evaluator(
                  to
                  , value.id
                  , value.hash!
                  , value
                  , (raw, t) => {
                      const got = value.get(raw, t);
                      return got ? 'true' : 'false';
                  });
            case DataType.bool:
                return value;
            case DataType.integer:
                return new Evaluator(
                  to
                  , value.id
                  , value.hash!
                  , value
                  , (raw, t) => {
                      const got = value.get(raw, t);
                      return got ? 1 : 0;
                  });
        }
        throw new Error('Unexpected cast error');
    }
}

export class ArrayType extends TypeBase<any[]> {

    public static matches(type: IType): type is ArrayType {
        return type.primary === DataType.array;
    }

    get primary(): DataType {
        if (this.list) {
            return DataType.list;
        }
        return DataType.array;
    }

    get name(): string {
        return this.of.name + '[]';
    }


    constructor(readonly of: _IType, private list: boolean) {
        super();
    }

    doCanCast(to: _IType) {
        if (to instanceof ArrayType) {
            return this.of.canCast(to.of);
        }
        return this.of.canCast(to);
    }

    doCast(value: Evaluator, _to: _IType) {
        if (_to instanceof ArrayType) {

            const to = _to as ArrayType;
            const valueType = value.type as ArrayType;
            return new Evaluator(
                to
                , value.id
                , value.hash!
                , value
                , (raw, t) => {
                    const arr = value.get(raw, t) as any[];
                    return arr.map(x => Value.constant(valueType.of, x).cast(to.of).get(raw, t));
                });
        }
        if (_to.primary === DataType.text) {
            return this.toText(_to, value);
        }
        return this.toSingleColumn(_to, value);
    }

    toText(to: _IType, value: Evaluator) {
        const valueType = value.type as ArrayType;
        const converter = Value.converter(valueType.of, Types.text());
        return new Evaluator(
            to
            , value.id
            , value.hash!
            , value
            , (raw, t) => {
                const arr = value.get(raw, t) as any[];
                const strs = arr.map(x => converter(x, t));
                const data = strs.join(',');
                return this.list
                    ? '(' + data + ')'
                    : '{' + data + '}';
            }, { forceNotConstant: true });
    }

    toSingleColumn(to: _IType, value: Evaluator) {
        const valueType = value.type as ArrayType;
        const converter = Value.converter(valueType.of, to);
        return new Evaluator(
            to
            , value.id
            , value.hash!
            , value
            , (raw, t) => {
                const arr = value.get(raw, t) as any[];
                return converter(arr[0], t);
            }, { forceNotConstant: true });
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
        if (nullIsh(elts)) {
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
                    .cast(this.of)
                    .get();
            }
        }
        return elts;
    }
}

export interface RecordCol {
    readonly name: string;
    readonly type: _IType;
}

/** Basic types */
export const Types = {
    [DataType.record]: (columns: RecordCol[]) => new RecordType(columns) as _IType,
    [DataType.bool]: new BoolType() as _IType,
    [DataType.text]: (len: number | nil = null) => makeText(len) as _IType,
    [DataType.citext]: new TextType(null, true),
    [DataType.timestamp]: (len: number | nil = null) => makeTimestamp(DataType.timestamp, len) as _IType,
    [DataType.timestamptz]: (len: number | nil = null) => makeTimestamp(DataType.timestamptz, len) as _IType,
    [DataType.uuid]: new UUIDtype() as _IType,
    [DataType.date]: new TimestampType(DataType.date) as _IType,
    [DataType.interval]: new IntervalType() as _IType,
    [DataType.time]: new TimeType(DataType.time) as _IType,
    [DataType.timetz]: new TimeType(DataType.timetz) as _IType,
    [DataType.jsonb]: new JSONBType(DataType.jsonb) as _IType,
    [DataType.regtype]: new RegTypeImpl() as _IType,
    [DataType.regclass]: new RegClassImpl() as _IType,
    [DataType.json]: new JSONBType(DataType.json) as _IType,
    [DataType.null]: new NullType() as _IType,
    [DataType.float]: new NumberType(DataType.float) as _IType,
    [DataType.integer]: new NumberType(DataType.integer) as _IType,
    [DataType.bigint]: new NumberType(DataType.bigint) as _IType,
    [DataType.bytea]: new ByteArrayType() as _IType,
    [DataType.point]: new PointType() as _IType,
    [DataType.line]: new LineType() as _IType,
    [DataType.lseg]: new LsegType() as _IType,
    [DataType.box]: new BoxType() as _IType,
    [DataType.inet]: new INetType() as _IType,
    [DataType.path]: new PathType() as _IType,
    [DataType.polygon]: new PolygonType() as _IType,
    [DataType.circle]: new CircleType() as _IType,
    default: new DefaultType() as _IType,
}

export const dateTypes: ReadonlySet<DataType> = new Set([
    DataType.timestamp
    , DataType.timestamptz
    , DataType.date
    , DataType.time
]);

export function isDateType(_type: _IType | DataType) {
    const t = typeof _type === 'string' ? _type : _type.primary;
    return dateTypes.has(t);
}

export function isGeometric(dt: DataType) {
    switch (dt) {
        case DataType.point:
        case DataType.line:
        case DataType.lseg:
        case DataType.box:
        case DataType.path:
        case DataType.polygon:
        case DataType.circle:
            return true;
    }
    return false;
}

const texts = new Map<number | null, _IType>();
function makeText(len: number | nil = null) {
    len = len ?? null;
    let got = texts.get(len);
    if (!got) {
        texts.set(len, got = new TextType(len));
    }
    return got;
}

const timestamps = new Map<string, _IType>();
function makeTimestamp(primary: DataType, len: number | nil = null) {
    len = len ?? null;
    const key = primary + '/' + len;
    let got = timestamps.get(key);
    if (!got) {
        timestamps.set(key, got = new TimestampType(primary, len));
    }
    return got;
}







export const typeSynonyms: { [key: string]: DataType | { type: DataType; ignoreConfig: boolean } } = {
    'varchar': DataType.text,
    'char': DataType.text,
    'character': DataType.text,
    'character varying': DataType.text,

    'int': DataType.integer,
    'int4': DataType.integer,
    'int8': DataType.bigint,
    'serial': DataType.integer,
    'serial8': DataType.bigint,
    'bigserial': DataType.integer,
    'smallserial': DataType.integer,
    'smallint': DataType.integer,
    'bigint': DataType.integer,
    'oid': DataType.integer,

    'decimal': DataType.float,
    'float': DataType.float,
    'double precision': DataType.float,
    'numeric': { type: DataType.float, ignoreConfig: true },
    'real': DataType.float,
    'money': DataType.float,

    'timestamp with time zone': DataType.timestamptz,
    'timestamp without time zone': DataType.timestamp,

    'boolean': DataType.bool,

    'time with time zone': DataType.timetz,
    'time without time zone': DataType.time,
}


/** Finds a common type by implicit conversion */
export function reconciliateTypes(values: IValue[], nullIfNoMatch?: false): _IType;
export function reconciliateTypes(values: IValue[], nullIfNoMatch: true): _IType | nil;
export function reconciliateTypes(values: IValue[], nullIfNoMatch?: boolean): _IType | nil
export function reconciliateTypes(values: IValue[], nullIfNoMatch?: boolean): _IType | nil {
    // FROM  https://www.postgresql.org/docs/current/typeconv-union-case.html

    const nonNull = values
        .filter(x => x.type.primary !== DataType.null);

    if (!nonNull.length) {
        // If all inputs are of type unknown, resolve as type text (the preferred type of the string category). Otherwise, unknown inputs are ignored for the purposes of the remaining rules.
        return Types.text();
    }

    // If all inputs are of the same type, and it is not unknown, resolve as that type.
    const single = new Set(nonNull
        .map(v => v.type.reg.typeId));
    if (single.size === 1) {
        return nonNull[0].type;
    }

    return reconciliateTypesRaw(nonNull, nullIfNoMatch);
}



/** Finds a common type by implicit conversion */
function reconciliateTypesRaw(values: IValue[], nullIfNoMatch?: false): _IType;
function reconciliateTypesRaw(values: IValue[], nullIfNoMatch: true): _IType | nil;
function reconciliateTypesRaw(values: IValue[], nullIfNoMatch?: boolean): _IType | nil
function reconciliateTypesRaw(values: IValue[], nullIfNoMatch?: boolean): _IType | nil {
    // find the matching type among non constants
    const foundType = values
        .reduce((final, c) => {
            if (c.type === Types.null) {
                return final;
            }
            const pref = final.prefer(c.type);
            if (!pref) {
                throw new CastError(c.type.primary, final.primary, c.id ?? undefined);
            }
            return pref;
        }, Types.null);

    // check that all constant literals are matching this.
    for (const x of values) {
        if (!x.isConstantLiteral && !x.type.canConvertImplicit(foundType)) {
            if (nullIfNoMatch) {
                return null;
            }
            throw new CastError(x.type.primary, foundType.primary);
        }
    }

    return foundType;
}
