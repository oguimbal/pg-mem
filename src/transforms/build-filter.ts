import { _ISelection, CastError, DataType, NotSupported } from '../interfaces-private';
import { buildValue } from '../predicate';
import { Types, makeArray } from '../datatypes';
import { EqFilter } from './eq-filter';
import { Value } from '../valuetypes';
import { FalseFilter } from './false-filter';
import { AndFilter } from './and-filter';
import { OrFilter } from './or-filter';
import { SeqScanFilter } from './seq-scan';
import { InFilter } from './in-filter';
import { NotInFilter } from './not-in-filter';
import { Expr, ExprBinary, ExprUnary, ExprTernary } from '../parser/syntax/ast';
import { StartsWithFilter } from './startswith-filter';
import { IneqFilter } from './ineq-filter';
import { hasNullish, nullIsh } from '../utils';
import { BetweenFilter } from './between-filter';

export function buildFilter<T>(this: void, on: _ISelection<T>, filter: Expr): _ISelection<T> {
    return _buildFilter(on, filter) ?? new SeqScanFilter(on, buildValue(on, filter))
}

function _buildFilter<T>(this: void, on: _ISelection<T>, filter: Expr): _ISelection<T> {
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
        return new EqFilter(built, true, 'eq');
    }

    // if this filter is a constant expression (ex: 1 = 1)
    // then return directly
    if (built.isConstant) {
        const val = built.convert(DataType.bool)
            .get();
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
        case 'ternary':
            return buildTernaryFilter(on, filter);
        default:
    }
}

function buildUnaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprUnary): _ISelection<T> {
    const { operand, op } = filter;
    switch (op) {
        case 'IS NULL':
        case 'IS NOT NULL': {
            const leftValue = buildValue(on, operand);
            if (leftValue.index) {
                return new EqFilter(leftValue, null, op === 'IS NULL' ? 'eq' : 'neq');
            }
            return new SeqScanFilter(on, Value.isNull(leftValue, op === 'IS NULL'));
        }
    }
}

function buildBinaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprBinary): _ISelection<T> {
    const { left, right, op } = filter;
    switch (op) {
        case '=':
        case '!=':
        case '>':
        case '<':
        case '<=':
        case '>=':
            return buildComparison(on, filter);
        case 'AND':
        case 'OR': {
            const leftFilter = buildFilter(on, left);
            const rightFilter = buildFilter(on, right);
            if (leftFilter instanceof SeqScanFilter || rightFilter instanceof SeqScanFilter) {
                return null;
            }
            return op === 'AND'
                ? new AndFilter([leftFilter, rightFilter])
                : new OrFilter(leftFilter, rightFilter);
        }
        case 'IN':
        case 'NOT IN': {
            const value = buildValue(on, left);
            let arrayValue = buildValue(on, right);
            // to support things like: "col in (value)" - which RHS does not parse to an array
            if (arrayValue.type.primary !== DataType.array) {
                arrayValue = Value.array([arrayValue]);
            }
            const array = arrayValue.convert(makeArray(value.type));
            // only support scanning indexes with one expression
            if (array.isConstant && value.index?.expressions.length === 1) {
                const arrCst = array.get();
                if (nullIsh(arrCst)) {
                    return new FalseFilter(on);
                }
                return op === 'IN'
                    ? new InFilter(value, arrCst)
                    : new NotInFilter(value, arrCst);
            }
            // todo use indexes on queries like "WHERE 'whatever' in (indexedOne, indexedTwo)"
            //   => this is an OrFilter
            return new SeqScanFilter(on, Value.in(value, array, op === 'IN'));
        }
        case 'LIKE': {
            const value = buildValue(on, left);
            if (value.index && value.index.expressions[0].hash === value.hash) {
                const valueToCompare = buildValue(on, right);
                if (valueToCompare.isConstant) {
                    const str = valueToCompare.get();
                    if (str === null) {
                        return new FalseFilter(on);
                    }
                    const got = /^([^%_]+)([%_]?.+)$/.exec(str);
                    if (got) {
                        const start = got[1];
                        if (start.length === str) {
                            // that's a full string with no tokens => just an '='
                            return buildComparison(on, {
                                type: 'binary',
                                op: '=',
                                left: left,
                                right: right,
                            });
                        }
                        // yea, we can use an index
                        const indexed = new StartsWithFilter(value, start);
                        if (got[2] === '%') {
                            // just a starsWith
                            return indexed;
                        }
                        // use index, but filter again on it.
                        return new SeqScanFilter(indexed, buildValue(on, filter));
                    }
                }
            }
        }
    }
}

function buildComparison<T>(this: void, on: _ISelection<T>, filter: ExprBinary): _ISelection<T> {
    const { op, left, right } = filter;
    let leftValue = buildValue(on, left);
    let rightValue = buildValue(on, right);

    if (leftValue.isConstant && rightValue.isConstant) {
        const global = buildValue(on, filter);
        const got = global.get();
        if (got) {
            return on;
        }
        return new FalseFilter(on);
    }

    switch (op) {
        case '=':
        case '!=': {
            if (leftValue.index && rightValue.isConstant) {
                return new EqFilter(leftValue, rightValue.get(), op === '=' ? 'eq' : 'neq')
            }
            if (rightValue.index && leftValue.isConstant) {
                return new EqFilter(rightValue, leftValue.get(), op === '=' ? 'eq' : 'neq');
            }
        }
        case '>':
        case '>=':
        case '<':
        case '<=':
            if (leftValue.index && leftValue.index.expressions[0].hash === leftValue.hash && rightValue.isConstant) {
                const fop = op === '>' ? 'gt'
                    : op === '>=' ? 'ge'
                        : op === '<' ? 'lt'
                            : 'le';
                return new IneqFilter(leftValue, fop, rightValue.get());
            }
            if (rightValue.index && rightValue.index.expressions[0].hash === rightValue.hash && leftValue.isConstant) {
                const fop = op === '>' ? 'le'
                    : op === '>=' ? 'lt'
                        : op === '<' ? 'ge'
                            : 'gt';
                return new IneqFilter(rightValue, fop, leftValue.get());
            }
    }
}

function buildTernaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprTernary): _ISelection<T> {
    switch (filter.op) {
        case 'BETWEEN':
        case 'NOT BETWEEN': {
            const value = buildValue(on, filter.value);
            const lo = buildValue(on, filter.lo);
            const hi = buildValue(on, filter.hi);
            const valueIndex = value.index;
            if (valueIndex && valueIndex.expressions[0].hash === value.hash && lo.isConstant && hi.isConstant) {
                const lov = lo.get();
                const hiv = hi.get();
                if (hasNullish(lov, hiv)) {
                    return new FalseFilter(on);
                }
                return new BetweenFilter(value, lov, hiv, filter.op === 'BETWEEN' ? 'inside' : 'outside');
            }
        }
    }
}