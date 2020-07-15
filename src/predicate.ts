import { _ISelection, IValue } from './interfaces-private';
import { NotSupported, trimNullish } from './utils';
import { DataType, CastError, QueryError } from './interfaces';
import { BoolValue, TextValue, NullValue, IsNullValue } from './datatypes';
import hash from 'object-hash';


export function buildValue(data: _ISelection, val: any): IValue {
    val = trimNullish(val);
    return _buildValue(data, val);
}

function _buildValue(data: _ISelection, val: any): IValue {
    switch (val.type) {
        case 'binary_expr':
            return buildBinary(data, val.left, val.operator, val.right);
        case 'column_ref':
            return data.getColumn(val.column);
        case 'string':
        case 'single_quote_string':
            return TextValue.constant(val.value);
        case 'null':
            return NullValue.constant();
        case 'expr_list':
            const vals = (val.value as any[]).map(x => _buildValue(data, x));
            flkdjsf
        default:
            throw new NotSupported('condition ' + val.type);
    }
}


function buildBinary(data: _ISelection, left: any, operator: string, right: any): IValue {
    let leftValue = _buildValue(data, left);
    let rightValue = _buildValue(data, right);
    if (rightValue.canConvert(leftValue.type)) {
        rightValue = rightValue.convert(leftValue.type);
    } else if (leftValue.canConvert(rightValue.type)) {
        leftValue = leftValue.convert(rightValue.type);
    } else {
        throw new CastError(leftValue.type, rightValue.type);
    }

    let getter: (a: any, b: any) => boolean;
    switch (operator) {
        case '=':
            getter = (a, b) => leftValue.equals(a, b);
            break;
        case '!=':
        case '<>': // ?
            getter = (a, b) => !leftValue.equals(a, b);
            break;
        case '>':
            getter = (a, b) => leftValue.gt(a, b);
            break;
        case '<':
            getter = (a, b) => leftValue.lt(a, b);
            break;
        case '>=':
            getter = (a, b) => leftValue.gt(a, b) || leftValue.equals(a, b);
            break;
        case '<=':
            getter = (a, b) => leftValue.lt(a, b) || leftValue.equals(a, b);
            break;
        case 'AND':
        case 'OR':
            if (!leftValue.canConvert(DataType.bool)) {
                throw new CastError(leftValue.type, DataType.bool);
            }
            if (!rightValue.canConvert(DataType.bool)) {
                throw new CastError(rightValue.type, DataType.bool);
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
            if (!(rightValue instanceof NullValue)) {
                throw new NotSupported('Onlys supports IS NULL operator');
            }
            return IsNullValue.of(leftValue, operator === 'IS');
        default:
            throw new NotSupported('operator ' + operator);
    }

    const sql = `${leftValue.id} ${operator} ${rightValue.id}`;
    const hashed = hash({ left: left.hash, operator, right: right.hash });
    return new BoolValue(null
        , sql
        , hashed
        , data
        , raw => {
            const leftRaw = leftValue.get(raw);
            const rightRaw = rightValue.get(raw);
            return getter(leftRaw, rightRaw);
        });
}
