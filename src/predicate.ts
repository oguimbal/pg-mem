import { _ISelection, IValue, _IType } from './interfaces-private';
import { NotSupported, trimNullish } from './utils';
import { DataType, CastError, QueryError } from './interfaces';
import hash from 'object-hash';
import { Value, Evaluator } from './valuetypes';
import { Types } from './datatypes';
import { Query } from './query';


export function buildValue(data: _ISelection, val: any): IValue {
    val = trimNullish(val);
    return _buildValue(data, val);
}

function _buildValue(data: _ISelection, val: any): IValue {
    switch (val.type) {
        case 'binary_expr':
            if (val.operator === 'IN' || val.operator === 'NOT IN') {
                return buildIn(data, val.left, val.right, val.operator === 'IN');
            }
            return buildBinary(data, val.left, val.operator, val.right);
        case 'column_ref':
            return data.getColumn(val.column);
        case 'string':
        case 'single_quote_string':
            return Value.text(val.value);
        case 'null':
            return Value.null();
        case 'expr_list':
            const vals = (val.value as any[]).map(x => _buildValue(data, x));
            return Value.array(vals);
        default:
            throw new NotSupported('condition ' + val.type);
    }
}

function buildIn(data: _ISelection, left: any, array: any, inclusive: boolean): IValue {
    let leftValue = _buildValue(data, left);
    let rightValue = _buildValue(data, array);
    return Value.in(leftValue, rightValue, inclusive);
}

function buildBinary(data: _ISelection, left: any, operator: string, right: any): IValue {
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

    let getter: (a: any, b: any) => boolean;
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
        default:
            throw new NotSupported('operator ' + operator);
    }

    const sql = `${leftValue.id} ${operator} ${rightValue.id}`;
    const hashed = hash({ left: left.hash, operator, right: right.hash });
    return new Evaluator(
        Types.bool
        , null
        , sql
        , hashed
        , data
        , raw => {
            const leftRaw = leftValue.get(raw);
            const rightRaw = rightValue.get(raw);
            return getter(leftRaw, rightRaw);
        });
}
