import { _ISelection, CastError, DataType, NotSupported } from '../interfaces-private';
import { buildValue } from '../predicate';
import { Types, makeArray } from '../datatypes';
import { EqFilter } from './eq-filter';
import { Value } from '../valuetypes';
import { FalseFilter } from './false-filter';
import { buildAndFilter } from './and-filter';
import { OrFilter } from './or-filter';
import { NeqFilter } from './neq-filter';
import { SeqScanFilter } from './seq-scan';
import { InFilter } from './in-filter';
import { NotInFilter } from './not-in-filter';
import { Expr, ExprBinary, ExprUnary } from '../parser/syntax/ast';

export function buildFilter<T>(this: void, on: _ISelection<T>, filter: Expr): _ISelection<T> {

    // check if there is a direct index
    const built = buildValue(on, filter);
    if (built.index) {
        if (built.index.expressions.length !== 1) {
            throw new Error('Was not expecing multiples expressions filter');
        }
        const itype = built.index.expressions[0].type;
        if (itype !== Types.bool) {
            throw new CastError(itype.primary, DataType.bool);
        }
        return new EqFilter(built, [Value.bool(true)]);
    }

    // if this filter is a constant expression (ex: 1 = 1)
    // then return directly
    if (built.isConstant) {
        const val = built.convert(DataType.bool)
            .get(null);
        if (val) {
            return on;
        }
        return new FalseFilter(on);
    }
    switch (filter.type) {
        case 'binary':
            return buildBinaryFilter(on, filter);
        case 'unary':
            return buildUnaryFilter(on, filter);
        default:
            throw new NotSupported('condition ' + filter.type);
    }
}

function buildUnaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprUnary): _ISelection<T> {
    const { operand, op } = filter;
    switch (op) {
        case 'IS NULL':
        case 'IS NOT NULL': {
            const leftValue = buildValue(on, operand);
            if (leftValue.index) {
                return op === 'IS NULL'
                    ? new EqFilter(leftValue, [Value.null()])
                    : new NeqFilter(leftValue, [Value.null()]);
            }
            return new SeqScanFilter(on, Value.isNull(leftValue, op === 'IS NULL'));
        }
    }
}

function buildBinaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprBinary): _ISelection<T> {
    const { left, right, op } = filter;
    switch (op) {
        case '=':
        case '>':
        case '<':
        case '<=':
        case '>=':
            return buildComparison(on, filter);
        case 'AND':
        case 'OR':
            const leftFilter = buildFilter(on, left);
            const rightFilter = buildFilter(on, right);
            return op === 'AND'
                ? buildAndFilter(leftFilter, rightFilter)
                : new OrFilter(leftFilter, rightFilter);
        case 'IN':
        case 'NOT IN':
            const value = buildValue(on, left);
            const array = buildValue(on, right).convert(makeArray(value.type));
            // only support scanning indexes with one expression
            if (array.isConstant && value.index?.expressions.length === 1) {
                return op === 'IN'
                    ? new InFilter(value, array)
                    : new NotInFilter(value, array);
            }
            // todo use indexes on queries like "WHERE 'whatever' in (indexedOne, indexedTwo)"
            //   => this is an OrFilter
            return new SeqScanFilter(on, Value.in(value, array, op === 'IN'));
        default:
            return new SeqScanFilter(on, buildValue(on, filter));
    }
}

function buildComparison<T>(this: void, on: _ISelection<T>, filter: ExprBinary): _ISelection<T> {
    const { op, left, right } = filter;
    let leftValue = buildValue(on, left);
    let rightValue = buildValue(on, right);

    if (leftValue.isConstant && rightValue.isConstant) {
        const global = buildValue(on, filter);
        const got = global.get(null);
        if (got) {
            return on;
        }
        return new FalseFilter(on);
    }

    if (op === '=' || op === '!=') {
        if (leftValue.index && rightValue.isConstant) {
            return op === '='
                ? new EqFilter(leftValue, [rightValue])
                : new NeqFilter(leftValue, [rightValue])
        }
        if (rightValue.index && leftValue.isConstant) {
            return op === '='
                ? new EqFilter(rightValue, [leftValue])
                : new NeqFilter(rightValue, [leftValue]);
        }
    }

    return new SeqScanFilter(on, buildValue(on, filter));
}