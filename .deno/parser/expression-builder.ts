import { _ISelection, IValue, _IType, _ISchema, _IAlias } from '../interfaces-private.ts';
import { buildLikeMatcher, nullIsh, hasNullish, intervalToSec, parseTime, asSingleQName, colToStr } from '../utils.ts';
import { DataType, CastError, QueryError, NotSupported, nil, ColumnNotFound } from '../interfaces.ts';
import hash from 'https://deno.land/x/object_hash@2.0.3.1/mod.ts';
import { Value, Evaluator } from '../evaluator.ts';
import { Types, isNumeric, reconciliateTypes, ArrayType, RecordCol } from '../datatypes/index.ts';
import { Expr, ExprBinary, UnaryOperator, ExprCase, ExprWhen, ExprMember, ExprArrayIndex, ExprTernary, BinaryOperator, SelectStatement, ExprValueKeyword, ExprExtract, Interval, ExprOverlay, ExprSubstring, ExprCall } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import lru from 'https://deno.land/x/lru_cache@6.0.0-deno.4/mod.ts';
import { aggregationFunctions, getAggregator } from '../transforms/aggregation.ts';
import moment from 'https://deno.land/x/momentjs@2.29.1-deno/mod.ts';
import { IS_PARTIAL_INDEXING } from '../execution/clean-results.ts';
import { buildCtx } from './context.ts';
import { buildSelect } from '../execution/select.ts';


const builtLru = new lru<_ISelection | null, lru<Expr, IValue>>({
    max: 30,
});
export function buildValue(val: Expr): IValue {
    const ret = _buildValue(val);
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

function _buildValue(val: Expr): IValue {
    // cache expressions build (they almost are always rebuilt several times in a row)
    const data = buildCtx().selection;
    let selLru = builtLru.get(data ?? null);
    let got: IValue | nil;
    if (selLru) {
        got = selLru.get(val);
        if (got) {
            return got;
        }
    }
    got = _buildValueReal(val);
    if (data.isAggregation()) {
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

function _buildValueReal(val: Expr): IValue {
    const { schema, getParameter, selection } = buildCtx();
    switch (val.type) {
        case 'binary':
            if (val.op === 'IN' || val.op === 'NOT IN') {
                return buildIn(val.left, val.right, val.op === 'IN');
            }
            return buildBinary(val);
        case 'unary':
            return buildUnary(val.op, val.operand);
        case 'ref':
            // try to get a column reference
            // todo refactor getColumn() to NameResolvers
            const found = selection.getColumn(val, true);
            if (found) {
                return found;
            }
            // try to get a parameter reference
            const arg = !val.table && getParameter(val.name);
            if (arg) {
                return arg;
            }
            // try to select an aliased record (= a table)
            const alias = !val.table && selection.selectAlias(val.name);
            if (alias) {
                return buildRecord(alias);
            }
            throw new ColumnNotFound(colToStr(val));
        case 'string':
            return Value.text(val.value);
        case 'null':
            return Value.null();
        case 'list':
        case 'array':
            const vals = val.expressions.map(x => _buildValue(x));
            return val.type === 'list'
                ? Value.list(vals)
                : Value.array(vals);
        case 'numeric':
            return Value.number(val.value);
        case 'integer':
            return Value.number(val.value, Types.integer);
        case 'call':
            return _buildCall(val);
        case 'cast':
            return _buildValue(val.operand)
                .cast(schema.getType(val.to))
        case 'case':
            return buildCase(val);
        case 'member':
            return buildMember(val);
        case 'arrayIndex':
            return buildArrayIndex(val);
        case 'boolean':
            return Value.bool(val.value);
        case 'ternary':
            return buildTernary(val);
        case 'select':
        case 'union':
        case 'union all':
        case 'with':
        case 'with recursive':
        case 'values':
            return buildSelectAsArray(val);
        case 'array select':
            return buildSelectAsArray(val.select);
        case 'constant':
            return Value.constant(val.dataType as any, val.value);
        case 'keyword':
            return buildKeyword(val, []);
        case 'parameter':
            const [_, n] = /^\$(\d+)$/.exec(val.name) ?? [];
            if (!n) {
                throw new Error('Unexpected parameter ref shape: ' + val.name);
            }
            return getParameter(parseInt(n) - 1)!;
        case 'extract':
            return buildExtract(val);
        case 'overlay':
            return buildOverlay(val);
        case 'substring':
            return buildSubstring(val);
        case 'default':
            throw new QueryError(`DEFAULT is not allowed in this context`, '42601');
        default:
            throw NotSupported.never(val);
    }
}

function buildRecord(alias: _IAlias): IValue {
    const cols = [...alias.listColumns()];
    return new Evaluator(
        Types.record(cols
            .map<RecordCol>(x => ({
                name: x.id!,
                type: x.type,
            })))
        , null
        , Math.random().toString() // must not be indexable => always different hash
        , []
        , (raw, t) => raw, { forceNotConstant: true });
}

function _buildCall(val: ExprCall): IValue {
    // if (typeof val.function !== 'string') {
    //     return buildKeyword( val.function, val.args);
    // }
    if (val.over) {
        throw new NotSupported('"OVER" clause is not implemented in pg-mem yet');
    }
    const nm = asSingleQName(val.function, 'pg_catalog');
    if (nm && aggregationFunctions.has(nm)) {
        const agg = getAggregator();
        if (!agg) {
            throw new QueryError(`aggregate functions are not allowed in WHERE`);
        }
        return agg.getAggregation(nm, val);
    }
    const args = val.args.map(x => _buildValue(x));
    return Value.function(val.function, args);
}

function buildKeyword(kw: ExprValueKeyword, args: Expr[]): IValue {
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
            return Value.constant(Types.text(), 'pg_mem');
        case 'current_schema':
            return Value.constant(Types.text(), 'public');
        case 'current_date':
            return Value.constant(Types.date, new Date());
        case 'current_timestamp':
        case 'localtimestamp':
            return Value.constant(Types.timestamp(), new Date());
        case 'localtime':
        case 'current_time':
            throw new NotSupported('"date" data type, please file an issue in https://github.com/oguimbal/pg-mem if you need it !');
        case 'distinct':
            throw new NotSupported(kw.keyword);
        default:
            throw NotSupported.never(kw.keyword);
    }
}

function buildUnary(op: UnaryOperator, operand: Expr) {
    const expr = _buildValue(operand);

    switch (op) {
        case 'IS NULL':
        case 'IS NOT NULL':
            return Value.isNull(expr, op === 'IS NULL');
        case 'IS TRUE':
        case 'IS NOT TRUE':
            return Value.isTrue(expr, op === 'IS TRUE');
        case 'IS FALSE':
        case 'IS NOT FALSE':
            return Value.isFalse(expr, op === 'IS FALSE');
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

function buildIn(left: Expr, array: Expr, inclusive: boolean): IValue {
    let leftValue = _buildValue(left);
    let rightValue = _buildValue(array);
    return Value.in(leftValue, rightValue, inclusive);
}


function buildBinary(val: ExprBinary): IValue {
    let leftValue = _buildValue(val.left);
    let rightValue = _buildValue(val.right);
    return buildBinaryValue(leftValue, val.op, rightValue);
}

export function buildBinaryValue(leftValue: IValue, op: BinaryOperator, rightValue: IValue): IValue {
    function expectSame() {
        const ll = leftValue.type.primary === DataType.list;
        const rl = rightValue.type.primary === DataType.list;
        if (ll !== rl) {
            function doMap(v: IValue): IValue {
                return v.map(x => {
                    if (!x) {
                        return x;
                    }
                    if (!Array.isArray(x)) {
                        // not supposed to happen
                        throw new Error(`Was expecting an array. Got instead ${x}`);
                    }
                    if (x.length > 1) {
                        throw new QueryError('more than one row returned by a subquery used as an expression', '21000');
                    }
                    return x[0];
                }, (v.type as ArrayType).of);
            }
            if (ll) {
                leftValue = doMap(leftValue);
            } else {
                rightValue = doMap(rightValue);
            }
        }
        const type: _IType = reconciliateTypes([leftValue, rightValue]);
        leftValue = leftValue.cast(type);
        rightValue = rightValue.cast(type);
        return type;
    }
    function expectBoth(t: _IType) {
        leftValue = leftValue.cast(t);
        rightValue = rightValue.cast(t);
    }

    let getter: (a: any, b: any) => any;
    let returnType: _IType = Types.bool;
    let commutative = true;
    let forcehash: any = null;
    let rejectNils = true;
    let impure = false;
    switch (op) {
        case '=': {
            const type = expectSame();
            getter = (a, b) => type.equals(a, b);
            break;
        }
        case '!=': {
            const type = expectSame();
            getter = (a, b) => {
                const ret = type.equals(a, b);
                return nullIsh(ret) ? null : !ret;
            };
            break;
        }
        case '>': {
            const type = expectSame();
            getter = (a, b) => type.gt(a, b);
            forcehash = { op: '>', left: leftValue.hash, right: rightValue.hash };
            break;
        }
        case '<': {
            const type = expectSame();
            getter = (a, b) => type.lt(a, b);
            forcehash = { op: '>', left: rightValue.hash, right: leftValue.hash };
            break;
        }
        case '>=': {
            const type = expectSame();
            getter = (a, b) => type.ge(a, b);
            forcehash = { op: '>=', left: leftValue.hash, right: rightValue.hash };
            break;
        }
        case '<=': {
            const type = expectSame();
            getter = (a, b) => type.le(a, b);
            forcehash = { op: '>=', left: rightValue.hash, right: leftValue.hash };
            break;
        }
        case 'AND':
        case 'OR':
            expectBoth(Types.bool);
            rejectNils = false;
            if (op === 'AND') {
                getter = (a, b) => a && b;
            } else {
                getter = (a, b) => a || b;
            }
            break;
        case '&&':
            if (leftValue.type.primary !== DataType.array || !rightValue.canCast(leftValue.type)) {
                throw new QueryError(`Operator does not exist: ${leftValue.type.name} && ${rightValue.type.name}`, '42883');
            }
            rightValue = rightValue.cast(leftValue.type);
            getter = (a, b) => a.some((element: any) => b.includes(element));
            break;
        case 'LIKE':
        case 'ILIKE':
        case 'NOT LIKE':
        case 'NOT ILIKE':
            expectBoth(Types.text());
            const caseSenit = op === 'LIKE' || op === 'NOT LIKE';
            const not = op === 'NOT ILIKE' || op === 'NOT LIKE';
            if (rightValue.isConstant) {
                const pattern = rightValue.get();
                if (nullIsh(pattern)) {
                    return Value.null(Types.bool);
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
        default: {
            const { schema } = buildCtx();
            const resolved = schema.resolveOperator(op, leftValue, rightValue);
            if (!resolved) {
                throw new QueryError(`operator does not exist: ${leftValue.type.name} ${op} ${rightValue.type.name}`, '42883');
            }
            leftValue = leftValue.cast(resolved.left);
            rightValue = rightValue.cast(resolved.right);
            commutative = resolved.commutative;
            returnType = resolved.returns;
            getter = resolved.implementation;
            rejectNils = !resolved.allowNullArguments;
            impure = !!resolved.impure;
            break;
        }
    }

    const hashed = hash(forcehash
        ?? (commutative
            ? { op, vals: [leftValue.hash, rightValue.hash].sort() }
            : { left: leftValue.hash, op, right: rightValue.hash }));

    // handle cases like:  blah = ANY(stuff)
    if (leftValue.isAny || rightValue.isAny) {
        return buildBinaryAny(leftValue, op, rightValue, returnType, getter, hashed);
    }

    return new Evaluator(
        returnType
        , null
        , hashed
        , [leftValue, rightValue]
        , (raw, t) => {
            const leftRaw = leftValue.get(raw, t);
            const rightRaw = rightValue.get(raw, t);
            if (rejectNils && (nullIsh(leftRaw) || nullIsh(rightRaw))) {
                return null;
            }
            return getter(leftRaw, rightRaw);
        }, impure ? { unpure: impure } : undefined);

}

function buildBinaryAny(leftValue: IValue, op: BinaryOperator, rightValue: IValue, returnType: _IType, getter: (a: any, b: any) => boolean, hashed: string) {
    if (leftValue.isAny && rightValue.isAny) {
        throw new QueryError('ANY() cannot be compared to ANY()');
    }
    if (returnType !== Types.bool) {
        throw new QueryError('Invalid ANY() usage');
    }
    return new Evaluator(
        returnType
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


function buildCase(op: ExprCase): IValue {
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
        when: buildValue(x.when).cast(Types.bool),
        then: buildValue(x.value)
    }));

    const valueType = reconciliateTypes(whenExprs.map(x => x.then));
    for (const v of whenExprs) {
        v.then = v.then.cast(valueType);
    }

    return new Evaluator(
        valueType
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

function buildMember(op: ExprMember): IValue {
    const oop = op.op;
    if (oop !== '->>' && oop !== '->') {
        throw NotSupported.never(oop);
    }
    const onExpr = buildValue(op.operand);
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
        op.op === '->' ? onExpr.type : Types.text()
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
                const i = op.member < 0
                    ? value.length + (op.member as number)
                    : op.member as number;
                return conv(value[i]);
            });
}


function buildArrayIndex(op: ExprArrayIndex): IValue {
    const onExpr = _buildValue(op.array);
    if (onExpr.type.primary !== DataType.array) {
        throw new QueryError(`Cannot use [] expression on type ${onExpr.type.primary}`);
    }
    const index = _buildValue(op.index).cast(Types.integer);
    return new Evaluator(
        (onExpr.type as ArrayType).of
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


function buildTernary(op: ExprTernary): IValue {
    const oop = op.op;
    if (oop !== 'NOT BETWEEN' && oop !== 'BETWEEN') {
        throw NotSupported.never(oop);
    }
    let value = _buildValue(op.value);
    let hi = _buildValue(op.hi);
    let lo = _buildValue(op.lo);
    const type = reconciliateTypes([value, hi, lo]);
    value = value.cast(type);
    hi = hi.cast(type);
    lo = lo.cast(type);
    const conv = oop === 'NOT BETWEEN'
        ? (x: boolean) => !x
        : (x: boolean) => x;

    return new Evaluator(
        Types.bool
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


function buildSelectAsArray(op: SelectStatement): IValue {
    // todo: handle refs to 'data' in op statement.
    //  ... and refactor this. This is way too hacky to be maintainable
    //   (this wont allow the subrequest to access outer context, for instance)
    const onData = buildSelect(op);
    if (onData.columns.length !== 1) {
        throw new QueryError('subquery must return only one column', '42601');
    }
    return new Evaluator(
        onData.columns[0].type.asList()
        , null
        , Math.random().toString() // must not be indexable => always different hash
        , null // , onData.columns[0]
        , (raw, t) => {
            const ret = [];
            for (const v of onData.enumerate(t!)) {
                ret.push(onData.columns[0].get(v, t));
            }
            return ret;
        }, {
        forceNotConstant: true
    });
}


function buildExtract(op: ExprExtract): IValue {
    const from = _buildValue(op.from);
    function extract(as: _IType, fn: (v: any) => any, result = Types.integer) {
        const conv = from.cast(as);
        return new Evaluator(
            result
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
            if (from.canCast(Types.date)) {
                return extract(Types.date, x => moment.utc(x).date());
            }
            return extract(Types.interval, (x: Interval) => x.days ?? 0);
        case 'second':
            if (from.canCast(Types.time)) {
                return extract(Types.time, x => {
                    const t = parseTime(x);
                    return t.second() + t.milliseconds() / 1000;
                }, Types.float);
            }
            return extract(Types.interval, (x: Interval) => (x.seconds ?? 0) + (x.milliseconds ?? 0) / 1000, Types.float);
        case 'minute':
            if (from.canCast(Types.time)) {
                return extract(Types.time, x => parseTime(x).minute());
            }
            return extract(Types.interval, (x: Interval) => x.minutes ?? 0);
        case 'milliseconds':
            if (from.canCast(Types.time)) {
                return extract(Types.time, x => {
                    const t = parseTime(x);
                    return t.seconds() * 1000 + t.milliseconds();
                });
            }
            return extract(Types.interval, (x: Interval) => (x.seconds ?? 0) * 1000 + (x.milliseconds ?? 0), Types.float);
        case 'month':
            if (from.canCast(Types.date)) {
                return extract(Types.date, x => moment.utc(x).month() + 1);
            }
            return extract(Types.interval, (x: Interval) => x.months ?? 0);
        case 'year':
            if (from.canCast(Types.date)) {
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
            if (from.canCast(Types.timestamp())) {
                return extract(Types.timestamp(), x => moment.utc(x).unix(), Types.float);
            }
            return extract(Types.interval, (x: Interval) => intervalToSec(x));
        case 'hour':
            if (from.canCast(Types.timestamp())) {
                return extract(Types.timestamp(), x => moment.utc(x).hour());
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
            if (from.canCast(Types.time)) {
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


function buildOverlay(op: ExprOverlay): IValue {
    const value = _buildValue(op.value).cast(Types.text());
    const placing = _buildValue(op.placing).cast(Types.text());
    const from = _buildValue(op.from).cast(Types.integer);
    const forr = op.for && _buildValue(op.for).cast(Types.integer);

    return new Evaluator(
        Types.text()
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

function buildSubstring(op: ExprSubstring): IValue {
    const value = _buildValue(op.value).cast(Types.text());
    const vals = [value];
    const from = op.from && _buildValue(op.from).cast(Types.integer);
    const forr = op.for && _buildValue(op.for).cast(Types.integer);
    if (forr) {
        vals.push(forr);
    }
    if (from) {
        vals.push(from);
    }

    return new Evaluator(
        Types.text()
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
