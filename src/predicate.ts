import { _ISelection, IValue, _IType } from './interfaces-private';
import { trimNullish, queryJson, buildLikeMatcher, nullIsh, hasNullish } from './utils';
import { DataType, CastError, QueryError, IType, NotSupported } from './interfaces';
import hash from 'object-hash';
import { Value, Evaluator } from './valuetypes';
import { Types, isNumeric, isInteger, singleSelection, fromNative, reconciliateTypes, ArrayType, makeArray } from './datatypes';
import { Expr, ExprBinary, UnaryOperator, ExprCase, ExprWhen, ExprMember, ExprArrayIndex, ExprTernary, BinaryOperator, SelectStatement } from './parser/syntax/ast';
import lru from 'lru-cache';
import { buildFilter } from './transforms/build-filter';


const builtLru = new lru<Expr, IValue>({
    max: 500,
})
export function buildValue(data: _ISelection, val: Expr): IValue {
    return _buildValue(data, val);
}

function _buildValue(data: _ISelection, val: Expr): IValue {
    // cache expressions build (they almost are always rebuilt several times in a row)
    let got = builtLru.get(val);
    if (got) {
        return got;
    }
    builtLru.set(val, got = _buildValueReal(data, val));
    return got;
}

function _buildValueReal(data: _ISelection, val: Expr): IValue {
    switch (val.type) {
        case 'binary':
            if (val.op === 'IN' || val.op === 'NOT IN') {
                return buildIn(data, val.left, val.right, val.op === 'IN');
            }
            return buildBinary(data, val);
        case 'unary':
            return buildUnary(data, val.op, val.operand);
        case 'ref':
            return val.table
                ? data.getColumn(val.table + '.' + val.name)
                : data.getColumn(val.name);
        case 'string':
            return Value.text(val.value);
        case 'null':
            return Value.null();
        case 'list':
            const vals = val.expressions.map(x => _buildValue(data, x));
            return Value.array(vals);
        case 'numeric':
            return Value.number(val.value);
        case 'integer':
            return Value.number(val.value, Types.int);
        case 'call':
            const args = val.args.map(x => _buildValue(data, x));
            return Value.function(val.function, args);
        case 'cast':
            return _buildValue(data, val.operand)
                .convert(fromNative(val.to))
        case 'case':
            return buildCase(data, val);
        case 'member':
            return buildMember(data, val);
        case 'arrayIndex':
            return buildArrayIndex(data, val);
        case 'boolean':
            return Value.bool(val.value);
        case 'ternary':
            return buildTernary(data, val);
        case 'select':
            return buildSelectAsArray(data, val);
        default:
            throw NotSupported.never(val);
    }
}

function buildUnary(data: _ISelection, op: UnaryOperator, operand: Expr) {
    const expr = _buildValue(data, operand);

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
            expr
                .convert(DataType.bool)
                .setId
        case '-':
            return Value.negate(expr);
        default:
            throw NotSupported.never(op, 'Unary operator not supported');
    }
}

function buildIn(data: _ISelection, left: Expr, array: Expr, inclusive: boolean): IValue {
    let leftValue = _buildValue(data, left);
    let rightValue = _buildValue(data, array);
    return Value.in(leftValue, rightValue, inclusive);
}

function nop(ignore) {
    // do nothing
}

function buildBinary(data: _ISelection, val: ExprBinary): IValue {
    const { left, right, op } = val;
    let leftValue = _buildValue(data, left);
    let rightValue = _buildValue(data, right);
    const type: _IType = reconciliateTypes([leftValue, rightValue]);
    leftValue = leftValue.convert(type);
    rightValue = rightValue.convert(type);

    let getter: (a: any, b: any) => any;
    let returnType: _IType = Types.bool;
    switch (op) {
        case '=':
            getter = (a, b) => type.equals(a, b);
            break;
        case '!=':
            getter = (a, b) => !type.equals(a, b);
            break;
        case '>':
            getter = (a, b) => type.gt(a, b);
            break;
        case '<':
            getter = (a, b) => type.lt(a, b);
            break;
        case '>=':
            getter = (a, b) => type.gt(a, b) || type.equals(a, b);
            break;
        case '<=':
            getter = (a, b) => type.lt(a, b) || type.equals(a, b);
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
                    break;
                case '-':
                    getter = (a, b) => a - b;
                    break;
                case '*':
                    getter = (a, b) => a * b;
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
            if (!leftValue.canConvert(DataType.bool)) {
                throw new CastError(leftValue.type.primary, DataType.bool);
            }
            if (!rightValue.canConvert(DataType.bool)) {
                throw new CastError(rightValue.type.primary, DataType.bool);
            }
            leftValue = leftValue.convert(DataType.bool);
            rightValue = rightValue.convert(DataType.bool);

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
                    return Value.null(Types.bool);
                }
                let matcher: (str: string) => boolean;
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
                getter = not
                    ? a => nullIsh(a) ? true : !matcher(a)
                    : a => nullIsh(a) ? null : matcher(a);
            } else {
                getter = not
                    ? (a, b) => nullIsh(a) ? true : !buildLikeMatcher(b, caseSenit)(a)
                    : (a, b) => hasNullish(a, b) ? null : buildLikeMatcher(b, caseSenit)(a);
            }
            break;
        default:
            throw NotSupported.never(op, 'operator');
    }

    const sql = `${leftValue.id} ${op} ${rightValue.id}`;
    const hashed = hash({ left: leftValue.hash, op, right: rightValue.hash });

    // handle cases like:  blah = ANY(stuff)
    if (leftValue.isAny || rightValue.isAny) {
        return buildBinaryAny(leftValue, op, rightValue, returnType, getter, sql, hashed);
    }

    return new Evaluator(
        returnType
        , null
        , sql
        , hashed
        , singleSelection([leftValue, rightValue])
        , (raw, t) => {
            const leftRaw = leftValue.get(raw, t);
            const rightRaw = rightValue.get(raw, t);
            return getter(leftRaw, rightRaw);
        }).asConstant(allConstants(leftValue, rightValue));

}

function buildBinaryAny(leftValue: IValue, op: BinaryOperator, rightValue: IValue, returnType: _IType, getter: (a: any, b: any) => boolean, sql: string, hashed: string) {
    if (leftValue.isAny && rightValue.isAny) {
        throw new QueryError('ANY() cannot be compared to ANY()');
    }
    if (returnType !== Types.bool) {
        throw new QueryError('Invalid ANY() usage');
    }
    return new Evaluator(
        returnType
        , null
        , sql
        , hashed
        , singleSelection([leftValue, rightValue])
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
            }).asConstant(allConstants(leftValue, rightValue))
}


function buildCase(data: _ISelection, op: ExprCase): IValue {
    const whens = !op.value
        ? op.whens
        : op.whens.map<ExprWhen>(v => ({
            when: {
                type: 'binary',
                op: '=',
                left: op.value,
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
        when: buildValue(data, x.when).convert(DataType.bool),
        then: buildValue(data, x.value)
    }));

    const valueType = reconciliateTypes(whenExprs.map(x => x.then));
    for (const v of whenExprs) {
        v.then = v.then.convert(valueType);
    }

    return new Evaluator(
        valueType
        , null
        , ['CASE'
            , whenExprs.map(x => `WHEN ${x.when.sql} THEN ${x.then.sql}`).join(' ')
            , ' END'].join(' ')
        , hash({ when: whenExprs.map(x => ({ when: x.when.hash, then: x.then.hash })) })
        , data
        , (raw, t) => {
            for (const w of whenExprs) {
                const cond = w.when.get(raw, t);
                if (cond) {
                    return w.then.get(raw, t);
                }
            }
            return null;
        }).asConstant(!whenExprs.some(x => !x.then?.isConstant || !x.when?.isConstant));
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
            if (x === null || x === undefined) {
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
        , `(${onExpr.sql})${op.op}${JSON.stringify(op.member)}`
        , hash([onExpr.hash, op.op, op.member])
        , data
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
                return conv(value[op.member]);
            }).asConstant(onExpr.isConstant);
}


function buildArrayIndex(data: _ISelection, op: ExprArrayIndex): IValue {
    const onExpr = _buildValue(data, op.array);
    if (onExpr.type.primary !== DataType.array) {
        throw new QueryError(`Cannot use [] expression on type ${onExpr.type.primary}`);
    }
    const index = _buildValue(data, op.index).convert(DataType.int);
    return new Evaluator(
        (onExpr.type as ArrayType).of
        , null
        , `(${onExpr.sql})[${index.sql}]`
        , hash({ array: onExpr.hash, index: index.hash })
        , data
        , (raw, t, isResult) => {
            const value = onExpr.get(raw, t);
            if (!Array.isArray(value)) {
                return null;
            }
            const i = index.get(raw, t);
            if (typeof i !== 'number' || i <= 0 || i > value.length) {
                return null;
            }
            const ret = value[i - 1]; // 1-base !
            if (isResult && Array.isArray(ret)) {
                return null;
            }
            return ret;
        }).asConstant(allConstants(onExpr, index));
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
        Types.bool
        , null
        , `${value.sql} BETWEEN ${lo.sql} AND ${hi.sql}`
        , hash({ value: value.hash, lo: lo.hash, hi: hi.hash })
        , data
        , (raw, t) => {
            const v = value.get(raw, t);
            if (v === null || v === undefined) {
                return null;
            }
            const lov = lo.get(raw, t);
            if (lov !== null && lov !== undefined && type.lt(v, lov)) {
                return conv(false);
            }
            const hiv = hi.get(raw, t);
            if (hiv !== null && hiv !== undefined && type.gt(v, hiv)) {
                return conv(false);
            }
            if ((lov ?? null) === null || (hiv ?? null) === null) {
                return null;
            }
            return conv(true);
        }
    ).asConstant(allConstants(value, hi, lo))
}

function allConstants(...exprs: IValue[]) {
    return !exprs.some(x => !x.isConstant);
}


function buildSelectAsArray(data: _ISelection, op: SelectStatement): IValue {
    const onData = data.subquery(data, op);
    if (onData.columns.length !== 1) {
        throw new QueryError('subquery has too many columns');
    }
    return new Evaluator(
        makeArray(onData.columns[0].type)
        , null
        , '<subselection>'
        , Math.random().toString() // must not be indexable => always different hash
        , null
        , (raw, t) => {
            const ret = [];
            for (const v of onData.enumerate(t)) {
                ret.push(onData.columns[0].get(v, t));
            }
            return ret;
        });
}