import { _ISelection, IValue, _IType, _ISchema } from './interfaces-private';
import { trimNullish, queryJson, buildLikeMatcher, nullIsh, hasNullish, intervalToSec, parseTime, asSingleQName } from './utils';
import { DataType, CastError, QueryError, IType, NotSupported, nil } from './interfaces';
import hash from 'object-hash';
import { Value, Evaluator } from './evaluator';
import { Types, isNumeric, isInteger, reconciliateTypes, ArrayType } from './datatypes';
import { Expr, ExprBinary, UnaryOperator, ExprCase, ExprWhen, ExprMember, ExprArrayIndex, ExprTernary, BinaryOperator, SelectStatement, ExprValueKeyword, ExprExtract, parseIntervalLiteral, Interval, ExprOverlay, ExprSubstring } from 'pgsql-ast-parser';
import lru from 'lru-cache';
import { aggregationFunctions, Aggregation } from './transforms/aggregation';
import moment from 'moment';
import { IS_PARTIAL_INDEXING } from './clean-results';


const builtLru = new lru<_ISelection | null, lru<Expr, IValue>>({
    max: 30,
});
export function buildValue(data: _ISelection, val: Expr): IValue {
    const ret = _buildValue(data, val);
    checkNotUntypedArray(ret);
    return ret;
}


function checkNotUntypedArray(value: IValue) {
    // A bit ugly: check that this is not a non typed array (empty array)
    // see https://github.com/oguimbal/pg-mem/issues/64
    // + corresponding UTs
    const type = value.type;
    if (type instanceof ArrayType && type.of == Types.null) {
        throw new QueryError(`cannot determine type of empty array`);
    }
}

export function uncache(data: _ISelection) {
    builtLru.del(data);
}

function _buildValue(data: _ISelection | nil, val: Expr): IValue {
    // cache expressions build (they almost are always rebuilt several times in a row)
    let selLru = builtLru.get(data ?? null);
    let got: IValue | nil;
    if (selLru) {
        got = selLru.get(val);
        if (got) {
            return got;
        }
    }
    got = _buildValueReal(data!, val);
    if (data instanceof Aggregation) {
        got = data.checkIfIsKey(got);
    }
    if (!selLru) {
        builtLru.set(data ?? null, selLru = new lru({
            max: 50,
        }));
    }
    selLru.set(val, got);
    return got;
}

function _buildValueReal(data: _ISelection, val: Expr): IValue {
    if (!data) {
        debugger;
    }
    switch (val.type) {
        case 'binary':
            if (val.op === 'IN' || val.op === 'NOT IN') {
                return buildIn(data, val.left, val.right, val.op === 'IN');
            }
            return buildBinary(data, val);
        case 'unary':
            return buildUnary(data, val.op, val.operand);
        case 'ref':
            return data.getColumn(val);
        case 'string':
            return Value.text(data.ownerSchema, val.value);
        case 'null':
            return Value.null(data.ownerSchema);
        case 'list':
        case 'array':
            const vals = val.expressions.map(x => _buildValue(data, x));
            return Value.array(data.ownerSchema, vals, val.type === 'list');
        case 'numeric':
            return Value.number(data.ownerSchema, val.value);
        case 'integer':
            return Value.number(data.ownerSchema, val.value, Types.integer);
        case 'call':
            // if (typeof val.function !== 'string') {
            //     return buildKeyword(data.ownerSchema, val.function, val.args);
            // }
            const nm = asSingleQName(val.function, 'pg_catalog');
            if (nm && aggregationFunctions.has(nm)) {
                if (!(data instanceof Aggregation)) {
                    throw new QueryError(`aggregate functions are not allowed in WHERE`);
                }
                return data.getAggregation(nm, val);
            }
            const args = val.args.map(x => _buildValue(data, x));
            const schema = data.db.getSchema(val.function.schema);
            return Value.function(schema, val.function, args);
        case 'cast':
            return _buildValue(data, val.operand)
                .convert(data.ownerSchema.getType(val.to))
        case 'case':
            return buildCase(data, val);
        case 'member':
            return buildMember(data, val);
        case 'arrayIndex':
            return buildArrayIndex(data, val);
        case 'boolean':
            return Value.bool(data.ownerSchema, val.value);
        case 'ternary':
            return buildTernary(data, val);
        case 'select':
        case 'union':
        case 'union all':
        case 'with':
            return buildSelectAsArray(data, val);
        case 'array select':
            return buildSelectAsArray(data, val.select);
        case 'constant':
            return Value.constant(data.ownerSchema, val.dataType as any, val.value);
        case 'keyword':
            return buildKeyword(data.ownerSchema, val, []);
        case 'parameter':
            throw new NotSupported('Parameters expressions');
        case 'extract':
            return buildExtract(data, val);
        case 'overlay':
            return buildOverlay(data, val);
        case 'substring':
            return buildSubstring(data, val);
        default:
            throw NotSupported.never(val);
    }
}

function buildKeyword(schema: _ISchema, kw: ExprValueKeyword, args: Expr[]): IValue {
    if (args.length) {
        throw new NotSupported(`usage of "${kw.keyword}" keyword with arguments, please file an issue in https://github.com/oguimbal/pg-mem if you need it !`);
    }
    if (kw.type !== 'keyword') {
        throw new Error('Invalid AST');
    }
    switch (kw.keyword) {
        case 'current_catalog':
        case 'current_role':
        case 'current_user':
        case 'session_user':
        case 'user':
            return Value.constant(schema, Types.text(), 'pg_mem');
        case 'current_schema':
            return Value.constant(schema, Types.text(), 'public');
        case 'current_date':
            return Value.constant(schema, Types.date, new Date());
        case 'current_timestamp':
        case 'localtimestamp':
            return Value.constant(schema, Types.timestamp, new Date());
        case 'localtime':
        case 'current_time':
            throw new NotSupported('"date" data type, please file an issue in https://github.com/oguimbal/pg-mem if you need it !');
        case 'distinct':
            throw new NotSupported(kw.keyword);
        default:
            throw NotSupported.never(kw.keyword);
    }
}

function buildUnary(data: _ISelection, op: UnaryOperator, operand: Expr) {
    const expr = _buildValue(data, operand);

    switch (op) {
        case 'IS NULL':
        case 'IS NOT NULL':
            return Value.isNull(data.ownerSchema, expr, op === 'IS NULL');
        case 'IS TRUE':
        case 'IS NOT TRUE':
            return Value.isTrue(data.ownerSchema, expr, op === 'IS TRUE');
        case 'IS FALSE':
        case 'IS NOT FALSE':
            return Value.isFalse(data.ownerSchema, expr, op === 'IS FALSE');
        case '+':
            if (!isNumeric(expr.type)) {
                throw new CastError(expr.type.primary, DataType.float);
            }
            return expr;
        case 'NOT':
        case '-':
            return Value.negate(expr);
        default:
            throw NotSupported.never(op, 'Unary operator not supported');
    }
}

function buildIn(data: _ISelection, left: Expr, array: Expr, inclusive: boolean): IValue {
    let leftValue = _buildValue(data, left);
    let rightValue = _buildValue(data, array);
    return Value.in(data.ownerSchema, leftValue, rightValue, inclusive);
}


function buildBinary(data: _ISelection, val: ExprBinary): IValue {
    let leftValue = _buildValue(data, val.left);
    let rightValue = _buildValue(data, val.right);
    return buildBinaryValue(data, leftValue, val.op, rightValue);
}

export function buildBinaryValue(data: _ISelection, leftValue: IValue, op: BinaryOperator, rightValue: IValue): IValue {
    const type: _IType = reconciliateTypes([leftValue, rightValue]);
    leftValue = leftValue.convert(type);
    rightValue = rightValue.convert(type);

    let getter: (a: any, b: any) => any;
    let returnType: _IType = Types.bool;
    let commutative = true;
    let forcehash: any = null;
    switch (op) {
        case '=':
            getter = (a, b) => type.equals(a, b);
            break;
        case '!=':
            getter = (a, b) => {
                const ret = type.equals(a, b);
                return nullIsh(ret) ? null : !ret;
            };
            break;
        case '>':
            getter = (a, b) => type.gt(a, b);
            forcehash = { op: '>', left: leftValue.hash, right: rightValue.hash };
            break;
        case '<':
            getter = (a, b) => type.lt(a, b);
            forcehash = { op: '>', left: rightValue.hash, right: leftValue.hash };
            break;
        case '>=':
            getter = (a, b) => type.ge(a, b);
            forcehash = { op: '>=', left: leftValue.hash, right: rightValue.hash };
            break;
        case '<=':
            getter = (a, b) => type.le(a, b);
            forcehash = { op: '>=', left: rightValue.hash, right: leftValue.hash };
            break;
        case '+':
        case '-':
        case '*':
        case '/':
            if (!isNumeric(type)) {
                throw new QueryError(`Cannot apply ${op} on non numeric type ${type.primary}`);
            }
            returnType = type;
            switch (op) {
                case '+':
                    getter = (a, b) => a + b;
                    commutative = true;
                    break;
                case '-':
                    getter = (a, b) => a - b;
                    break;
                case '*':
                    getter = (a, b) => a * b;
                    commutative = true;
                    break;
                case '/':
                    if (isInteger(type)) {
                        getter = (a, b) => Math.trunc(a / b);
                    } else {
                        getter = (a, b) => a / b;
                    }
                    break;
            }
            break;
        case 'AND':
        case 'OR':
            leftValue = leftValue.convert(Types.bool);
            rightValue = rightValue.convert(Types.bool);

            if (op === 'AND') {
                getter = (a, b) => a && b;
            } else {
                getter = (a, b) => a || b;
            }
            break;
        case '@>':
            getter = (a, b) => queryJson(b, a);
            break;
        case '||':
            getter = (a, b) => a + b;
            returnType = Types.text();
            break;
        case 'LIKE':
        case 'ILIKE':
        case 'NOT LIKE':
        case 'NOT ILIKE':
            const caseSenit = op === 'LIKE' || op === 'NOT LIKE';
            const not = op === 'NOT ILIKE' || op === 'NOT LIKE';
            if (rightValue.isConstant) {
                const pattern = rightValue.get();
                if (pattern === null) {
                    return Value.null(data.ownerSchema, Types.bool);
                }
                let matcher: (str: string | number) => boolean | nil;
                if (rightValue.isAny) {
                    // handle LIKE ANY()
                    if (!Array.isArray(pattern)) {
                        throw new QueryError('Unsupported use of ANY()');
                    }
                    const patterns = pattern.map(x => buildLikeMatcher(x, caseSenit));
                    matcher = v => patterns.some(x => x(v));
                } else {
                    matcher = buildLikeMatcher(pattern, caseSenit);
                }
                getter = !not
                    ? a => nullIsh(a) ? null : matcher(a)
                    : a => {
                        if (nullIsh(a)) {
                            return null;
                        }
                        const val = matcher(a);
                        return nullIsh(val) ? null : !val;
                    };
            } else {
                getter = !not
                    ? (a, b) => hasNullish(a, b) ? null : buildLikeMatcher(b, caseSenit)(a)
                    : (a, b) => {
                        if (hasNullish(a, b)) {
                            return null;
                        }
                        const val = buildLikeMatcher(b, caseSenit)(a);
                        return nullIsh(val) ? null : !val;
                    };
            }
            break;
        default:
            // throw NotSupported.never(op, 'operator');
            throw new NotSupported('operator ' + op);
    }

    const hashed = hash(forcehash
        ?? (commutative
            ? { op, vals: [leftValue.hash, rightValue.hash].sort() }
            : { left: leftValue.hash, op, right: rightValue.hash }));

    // handle cases like:  blah = ANY(stuff)
    if (leftValue.isAny || rightValue.isAny) {
        return buildBinaryAny(data.ownerSchema, leftValue, op, rightValue, returnType, getter, hashed);
    }

    return new Evaluator(
        data.ownerSchema
        , returnType
        , null
        , hashed
        , [leftValue, rightValue]
        , (raw, t) => {
            const leftRaw = leftValue.get(raw, t);
            const rightRaw = rightValue.get(raw, t);
            if (nullIsh(leftRaw) || nullIsh(rightRaw)) {
                return null;
            }
            return getter(leftRaw, rightRaw);
        });

}

function buildBinaryAny(schema: _ISchema, leftValue: IValue, op: BinaryOperator, rightValue: IValue, returnType: _IType, getter: (a: any, b: any) => boolean, hashed: string) {
    if (leftValue.isAny && rightValue.isAny) {
        throw new QueryError('ANY() cannot be compared to ANY()');
    }
    if (returnType !== Types.bool) {
        throw new QueryError('Invalid ANY() usage');
    }
    return new Evaluator(
        schema
        , returnType
        , null
        , hashed
        , [leftValue, rightValue]
        , leftValue.isAny
            ? (raw, t) => {
                const leftRaw = leftValue.get(raw, t);
                if (nullIsh(leftRaw)) {
                    return null;
                }
                if (!Array.isArray(leftRaw)) {
                    throw new QueryError('Invalid ANY() usage: was expacting an array');
                }
                for (const lr of leftRaw) {
                    const rightRaw = rightValue.get(raw, t);
                    if (getter(lr, rightRaw)) {
                        return true;
                    }
                }
                return false;
            }
            : (raw, t) => {
                const rightRaw = rightValue.get(raw, t);
                if (nullIsh(rightRaw)) {
                    return null;
                }
                if (!Array.isArray(rightRaw)) {
                    throw new QueryError('Invalid ANY() usage: was expacting an array');
                }
                for (const rr of rightRaw) {
                    const leftRaw = leftValue.get(raw, t);
                    if (getter(leftRaw, rr)) {
                        return true;
                    }
                }
                return false;
            });
}


function buildCase(data: _ISelection, op: ExprCase): IValue {
    const whens = !op.value
        ? op.whens
        : op.whens.map<ExprWhen>(v => ({
            when: {
                type: 'binary',
                op: '=',
                left: op.value!,
                right: v.when,
            },
            value: v.value,
        }));
    if (op.else) {
        whens.push({
            when: { type: 'boolean', value: true },
            value: op.else,
        });
    }

    const whenExprs = whens.map(x => ({
        when: buildValue(data, x.when).convert(Types.bool),
        then: buildValue(data, x.value)
    }));

    const valueType = reconciliateTypes(whenExprs.map(x => x.then));
    for (const v of whenExprs) {
        v.then = v.then.convert(valueType);
    }

    return new Evaluator(
        data.ownerSchema
        , valueType
        , null
        , hash({ when: whenExprs.map(x => ({ when: x.when.hash, then: x.then.hash })) })
        , [
            ...whenExprs.map(x => x.when),
            ...whenExprs.map(x => x.then)
        ]
        , (raw, t) => {
            for (const w of whenExprs) {
                const cond = w.when.get(raw, t);
                if (cond) {
                    return w.then.get(raw, t);
                }
            }
            return null;
        });
}

function buildMember(data: _ISelection, op: ExprMember): IValue {
    const oop = op.op;
    if (oop !== '->>' && oop !== '->') {
        throw NotSupported.never(oop);
    }
    const onExpr = buildValue(data, op.operand);
    if (onExpr.type !== Types.json && onExpr.type !== Types.jsonb) {
        throw new QueryError(`Cannot use member expression ${op.op} on type ${onExpr.type.primary}`);
    }

    const conv = op.op === '->'
        ? ((x: any) => x)
        : ((x: any) => {
            if (nullIsh(x)) {
                return null;
            }
            if (typeof x === 'string') {
                return x;
            }
            return JSON.stringify(x);
        });

    return new Evaluator(
        data.ownerSchema
        , op.op === '->' ? onExpr.type : Types.text()
        , null
        , hash([onExpr.hash, op.op, op.member])
        , onExpr
        , typeof op.member === 'string'
            ? (raw, t) => {
                const value = onExpr.get(raw, t);
                if (!value || typeof value !== 'object') {
                    return null;
                }
                return conv(value[op.member]);
            }
            : (raw, t) => {
                const value = onExpr.get(raw, t);
                if (!Array.isArray(value)) {
                    return null;
                }
                return conv(value[op.member as number]);
            });
}


function buildArrayIndex(data: _ISelection, op: ExprArrayIndex): IValue {
    const onExpr = _buildValue(data, op.array);
    if (onExpr.type.primary !== DataType.array) {
        throw new QueryError(`Cannot use [] expression on type ${onExpr.type.primary}`);
    }
    const index = _buildValue(data, op.index).convert(Types.integer);
    return new Evaluator(
        data.ownerSchema
        , (onExpr.type as ArrayType).of
        , null
        , hash({ array: onExpr.hash, index: index.hash })
        , [onExpr, index]
        , (raw, t) => {
            const value = onExpr.get(raw, t);
            if (!Array.isArray(value)) {
                return null;
            }
            const i = index.get(raw, t);
            if (typeof i !== 'number' || i <= 0 || i > value.length) {
                return null;
            }
            const ret = value[i - 1]; // 1-base !

            if (Array.isArray(ret)) {
                // ugly hack.. see clean-results.ts
                (ret as any)[IS_PARTIAL_INDEXING] = true;
            }
            return ret;
        });
}


function buildTernary(data: _ISelection, op: ExprTernary): IValue {
    const oop = op.op;
    if (oop !== 'NOT BETWEEN' && oop !== 'BETWEEN') {
        throw NotSupported.never(oop);
    }
    let value = _buildValue(data, op.value);
    let hi = _buildValue(data, op.hi);
    let lo = _buildValue(data, op.lo);
    const type = reconciliateTypes([value, hi, lo]);
    value = value.convert(type);
    hi = hi.convert(type);
    lo = lo.convert(type);
    const conv = oop === 'NOT BETWEEN'
        ? (x: boolean) => !x
        : (x: boolean) => x;

    return new Evaluator(
        data.ownerSchema
        , Types.bool
        , null
        , hash({ value: value.hash, lo: lo.hash, hi: hi.hash })
        , [value, hi, lo]
        , (raw, t) => {
            const v = value.get(raw, t);
            if (nullIsh(v)) {
                return null;
            }
            const lov = lo.get(raw, t);
            if (!nullIsh(lov) && type.lt(v, lov)) {
                return conv(false);
            }
            const hiv = hi.get(raw, t);
            if (!nullIsh(hiv) && type.gt(v, hiv)) {
                return conv(false);
            }
            if (nullIsh(lov) || nullIsh(hiv)) {
                return null;
            }
            return conv(true);
        }
    );
}


function buildSelectAsArray(data: _ISelection, op: SelectStatement): IValue {
    const onData = data.subquery(data, op);
    if (onData.columns.length !== 1) {
        throw new QueryError('subquery has too many columns');
    }
    return new Evaluator(
        data.ownerSchema
        , onData.columns[0].type.asList()
        , null
        , Math.random().toString() // must not be indexable => always different hash
        , onData.columns[0]
        , (raw, t) => {
            const ret = [];
            for (const v of onData.enumerate(t!)) {
                ret.push(onData.columns[0].get(v, t));
            }
            return ret;
        });
}


function buildExtract(data: _ISelection, op: ExprExtract): IValue {
    const from = _buildValue(data, op.from);
    function extract(as: _IType, fn: (v: any) => any, result = Types.integer) {
        const conv = from.convert(as);
        return new Evaluator(
            data.ownerSchema
            , result
            , null
            , hash({ extract: from.hash, field: op.field })
            , [conv]
            , (raw, t) => {
                const got = conv.get(raw, t);
                if (nullIsh(got)) {
                    return null;
                }
                return fn(got);
            }
        )
    }
    switch (op.field.name) {
        case 'millennium':
            return extract(Types.date, x => Math.ceil(moment.utc(x).year() / 1000));
        case 'century':
            return extract(Types.date, x => Math.ceil(moment.utc(x).year() / 100));
        case 'decade':
            return extract(Types.date, x => Math.floor(moment.utc(x).year() / 10));
        case 'day':
            if (from.canConvert(Types.date)) {
                return extract(Types.date, x => moment.utc(x).date());
            }
            return extract(Types.interval, (x: Interval) => x.days ?? 0);
        case 'second':
            if (from.canConvert(Types.time)) {
                return extract(Types.time, x => {
                    const t = parseTime(x);
                    return t.second() + t.milliseconds() / 1000;
                }, Types.float);
            }
            return extract(Types.interval, (x: Interval) => (x.seconds ?? 0) + (x.milliseconds ?? 0) / 1000, Types.float);
        case 'minute':
            if (from.canConvert(Types.time)) {
                return extract(Types.time, x => parseTime(x).minute());
            }
            return extract(Types.interval, (x: Interval) => x.minutes ?? 0);
        case 'milliseconds':
            if (from.canConvert(Types.time)) {
                return extract(Types.time, x => {
                    const t = parseTime(x);
                    return t.seconds() * 1000 + t.milliseconds();
                });
            }
            return extract(Types.interval, (x: Interval) => (x.seconds ?? 0) * 1000 + (x.milliseconds ?? 0), Types.float);
        case 'month':
            if (from.canConvert(Types.date)) {
                return extract(Types.date, x => moment.utc(x).month() + 1);
            }
            return extract(Types.interval, (x: Interval) => x.months ?? 0);
        case 'year':
            if (from.canConvert(Types.date)) {
                return extract(Types.date, x => moment.utc(x).year());
            }
            return extract(Types.interval, (x: Interval) => x.years ?? 0);
        case 'dow':
            return extract(Types.date, x => moment.utc(x).day());
        case 'isodow':
            return extract(Types.date, x => {
                const dow = moment.utc(x).day();
                return dow ? dow : 7;
            });
        case 'doy':
            return extract(Types.date, x => moment.utc(x).dayOfYear());
        case 'epoch':
            if (from.canConvert(Types.timestamp)) {
                return extract(Types.timestamp, x => moment.utc(x).unix(), Types.float);
            }
            return extract(Types.interval, (x: Interval) => intervalToSec(x));
        case 'hour':
            if (from.canConvert(Types.timestamp)) {
                return extract(Types.timestamp, x => moment.utc(x).hour());
            }
            return extract(Types.interval, (x: Interval) => x.hours ?? 0);
        case 'isoyear':
            return extract(Types.date, x => {
                const d = moment.utc(x);
                return d.dayOfYear() <= 1 ? d.year() - 1 : d.year();
            });
        case 'quarter':
            return extract(Types.date, x => moment.utc(x).quarter());
        case 'week':
            return extract(Types.date, x => moment.utc(x).week());
        case 'microseconds':
            if (from.canConvert(Types.time)) {
                return extract(Types.time, x => {
                    const t = parseTime(x);
                    return t.seconds() * 1000000 + t.milliseconds() * 1000;
                });
            }
            return extract(Types.interval, (x: Interval) => (x.seconds ?? 0) * 1000000 + (x.milliseconds ?? 0) * 1000);
        default:
            throw new NotSupported('Extract type "' + op.field + '"');
    }
}


function buildOverlay(data: _ISelection, op: ExprOverlay): IValue {
    const value = _buildValue(data, op.value).convert(Types.text());
    const placing = _buildValue(data, op.placing).convert(Types.text());
    const from = _buildValue(data, op.from).convert(Types.integer);
    const forr = op.for && _buildValue(data, op.for).convert(Types.integer);

    return new Evaluator(
        data.ownerSchema
        , Types.text()
        , null
        , hash({ overlay: value.hash, placing: placing.hash, from: from.hash, for: forr?.hash })
        , forr ? [value, placing, from, forr] : [value, placing, from]
        , (raw, t) => {
            const _value = value.get(raw, t) as string;
            if (nullIsh(_value)) {
                return null;
            }
            const _placing = placing.get(raw, t) as string;
            if (nullIsh(_placing)) {
                return null;
            }
            const _from = from.get(raw, t) as number;
            if (nullIsh(_from)) {
                return null;
            }
            const before = sqlSubstring(_value, 0, _from - 1);
            let after: string | nil;
            if (forr) {
                const _for = forr.get(raw, t) as number;
                if (nullIsh(_for)) {
                    return null;
                }
                after = sqlSubstring(_value, _from + _for);
            } else {
                after = sqlSubstring(_value, _placing.length + _from);
            }
            if (nullIsh(after)) {
                return null;
            }
            return before + _placing + after;
        });
}

function buildSubstring(data: _ISelection, op: ExprSubstring): IValue {
    const value = _buildValue(data, op.value).convert(Types.text());
    const vals = [value];
    const from = op.from && _buildValue(data, op.from).convert(Types.integer);
    const forr = op.for && _buildValue(data, op.for).convert(Types.integer);
    if (forr) {
        vals.push(forr);
    }
    if (from) {
        vals.push(from);
    }

    return new Evaluator(
        data.ownerSchema
        , Types.text()
        , null
        , hash({ substr: value.hash, from: from?.hash, for: forr?.hash })
        , vals
        , (raw, t) => {
            const _value = value.get(raw, t) as string;
            if (nullIsh(_value)) {
                return null;
            }
            let start = 0;
            let len: number | nil;
            if (from) {
                start = from.get(raw, t) as number;
                if (nullIsh(start)) {
                    return null;
                }
            }
            if (forr) {
                len = forr.get(raw, t) as number;
                if (nullIsh(len)) {
                    return null;
                }
            }
            return sqlSubstring(_value, start, len);
        });
}

export function sqlSubstring(value: string, from = 0, len?: number | nil): string | null {
    if (nullIsh(from) || nullIsh(value)) {
        return null;
    }
    // sql substring is base-1
    from--;
    if (from < 0) {
        from = 0;
    }
    if (!nullIsh(len)) {
        if (len! < 0) {
            throw new QueryError('negative substring length not allowed');
        }
        return value.substr(from, len!);
    }
    return value.substr(from);
}