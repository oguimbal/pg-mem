import { _ISelection, IValue, _IType } from './interfaces-private';
import { trimNullish, queryJson } from './utils';
import { DataType, CastError, QueryError, IType, NotSupported } from './interfaces';
import hash from 'object-hash';
import { Value, Evaluator } from './valuetypes';
import { Types, isNumeric, isInteger, singleSelection, fromNative } from './datatypes';
import { Query } from './query';
import { Expr, ExprBinary, UnaryOperator } from './parser/syntax/ast';


export function buildValue(data: _ISelection, val: Expr): IValue {
    return _buildValue(data, val);
}

function _buildValue(data: _ISelection, val: Expr): IValue {
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
        default:
            throw new NotSupported(val.type);
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
    let type: _IType;
    if (rightValue.canConvert(leftValue.type)) {
        rightValue = rightValue.convert(leftValue.type);
        type = leftValue.type;
    } else if (leftValue.canConvert(rightValue.type)) {
        leftValue = leftValue.convert(rightValue.type);
        type = rightValue.type;
    } else {
        throw new CastError(leftValue.type.primary, rightValue.type.primary);
    }

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
        default:
            throw new NotSupported('operator ' + op);
    }

    const sql = `${leftValue.id} ${op} ${rightValue.id}`;
    const hashed = hash({ left: leftValue.hash, op, right: rightValue.hash });
    return new Evaluator(
        returnType
        , null
        , sql
        , hashed
        , singleSelection([leftValue, rightValue])
        , raw => {
            const leftRaw = leftValue.get(raw);
            const rightRaw = rightValue.get(raw);
            return getter(leftRaw, rightRaw);
        });
}
