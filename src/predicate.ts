import { _ISelection, IValue, _IType } from './interfaces-private';
import { trimNullish, queryJson } from './utils';
import { DataType, CastError, QueryError, IType, NotSupported } from './interfaces';
import hash from 'object-hash';
import { Value, Evaluator } from './valuetypes';
import { Types, isNumeric, isInteger, singleSelection } from './datatypes';
import { Query } from './query';


export function buildValue(data: _ISelection, val: any): IValue {
    return _buildValue(data, val);
}

function _buildValue(data: _ISelection, val: any): IValue {
    switch (val.type) {
        case 'binary_expr':
            if (val.operator === 'IN' || val.operator === 'NOT IN') {
                return buildIn(data, val.left, val.right, val.operator === 'IN');
            }
            return buildBinary(data, val);
        case 'column_ref':
            return val.table
                ? data.getColumn(val.table + '.' + val.column)
                : data.getColumn(val.column);
        case 'string':
        case 'single_quote_string':
            return Value.text(val.value);
        case 'null':
            return Value.null();
        case 'expr_list':
            const vals = (val.value as any[]).map(x => _buildValue(data, x));
            return Value.array(vals);
        case 'number':
            return Value.number(val.value);
        case 'function':
            if (val.args.type !== 'expr_list') {
                throw new QueryError('Expecting expr_list as arguments');
            }
            const args = val.args.value.map(x => _buildValue(data, x));
            return Value.function(val.name, args);
        case 'unary_expr':
            const expr = _buildValue(data, val.expr);
            if (val.operator !== '-') {
                throw new NotSupported('Unary operator not supported: ' + val.operator)
            }
            return Value.negate(expr);
        default:
            throw new NotSupported('condition ' + val.type);
    }
}

function buildIn(data: _ISelection, left: any, array: any, inclusive: boolean): IValue {
    let leftValue = _buildValue(data, left);
    let rightValue = _buildValue(data, array);
    return Value.in(leftValue, rightValue, inclusive);
}

function nop(ignore) {
    // do nothing
}

function buildBinary(data: _ISelection, val: any): IValue {
    const { left, right, operator, parentheses } = val;
    nop(parentheses); // <== just ignore that.
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
    switch (operator) {
        case '=':
            getter = (a, b) => type.equals(a, b);
            break;
        case '!=':
        case '<>': // ?
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
                throw new QueryError(`Cannot apply ${operator} on non numeric type ${type.primary}`);
            }
            returnType = type;
            switch (operator) {
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

            if (operator === 'AND') {
                getter = (a, b) => a && b;
            } else {
                getter = (a, b) => a || b;
            }
            break;
        case 'IS':
        case 'IS NOT':
            if (rightValue.type !== Types.null) {
                throw new NotSupported('Onlys supports IS NULL operator');
            }
            return Value.isNull(leftValue, operator === 'IS');
        case '@>':
            getter = (a, b) => queryJson(b, a);
            break;
        default:
            throw new NotSupported('operator ' + operator);
    }

    const sql = `${leftValue.id} ${operator} ${rightValue.id}`;
    const hashed = hash({ left: left.hash, operator, right: right.hash });
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
