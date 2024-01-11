import { _ISelection, CastError, DataType, NotSupported, IValue } from '../interfaces-private.ts';
import { buildValue } from '../parser/expression-builder.ts';
import { Types, ArrayType } from '../datatypes/index.ts';
import { EqFilter } from './eq-filter.ts';
import { Value } from '../evaluator.ts';
import { FalseFilter } from './false-filter.ts';
import { AndFilter } from './and-filter.ts';
import { OrFilter } from './or-filter.ts';
import { SeqScanFilter } from './seq-scan.ts';
import { InFilter } from './in-filter.ts';
import { NotInFilter } from './not-in-filter.ts';
import { Expr, ExprBinary, ExprUnary, ExprTernary } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { StartsWithFilter } from './startswith-filter.ts';
import { IneqFilter } from './ineq-filter.ts';
import { hasNullish, nullIsh } from '../utils.ts';
import { BetweenFilter } from './between-filter.ts';
import { QueryError } from '../interfaces.ts';
import { withSelection } from '../parser/context.ts';

export function buildFilter<T>(this: void, on: _ISelection<T>, filter: Expr, parentName: string): _ISelection<T> {
    return withSelection(on, () => {
        const where = buildValue(filter);
        if (!where.type.canConvertImplicit(Types.bool)) {
            throw new QueryError(`argument of ${parentName} must be type boolean, not type jsonb`, '42804');
        }
        return _buildFilter(on, filter, where) ?? new SeqScanFilter(on, where)
    });
}

function _buildFilter<T>(this: void, on: _ISelection<T>, filter: Expr, built: IValue): _ISelection<T> | null {
    // check if there is a direct index
    if (built.index) {
        if (built.index.expressions.length !== 1) {
            throw new Error('Was not expecing multiples expressions filter');
        }
        const itype = built.index.expressions[0].type;
        if (itype !== Types.bool) {
            throw new CastError(itype.primary, DataType.bool);
        }
        return new EqFilter(built, true, 'eq', false);
    }

    // if this filter is a constant expression (ex: 1 = 1)
    // then return directly
    if (built.isConstant) {
        const val = built.cast(Types.bool)
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
            return null;
    }
}

function buildUnaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprUnary): _ISelection<T> | null {
    const { operand, op } = filter;
    switch (op) {
        case 'IS NULL':
        case 'IS NOT NULL': {
            const leftValue = buildValue(operand);
            if (leftValue.index) {
                return new EqFilter(leftValue, null, op === 'IS NULL' ? 'eq' : 'neq', true);
            }
            return new SeqScanFilter(on, Value.isNull(leftValue, op === 'IS NULL'));
        }
    }
    return null;
}

function buildBinaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprBinary): _ISelection<T> | null {
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
            const leftFilter = buildFilter(on, left, op);
            const rightFilter = buildFilter(on, right, op);
            if (op === 'OR' && (leftFilter instanceof SeqScanFilter || rightFilter instanceof SeqScanFilter)) {
                return null;
            }
            return op === 'AND'
                ? new AndFilter([leftFilter, rightFilter])
                : new OrFilter(leftFilter, rightFilter);
        }
        case 'IN':
        case 'NOT IN': {
            const value = buildValue(left);
            let arrayValue = buildValue(right);
            // to support things like: "col in (value)" - which RHS does not parse to an array
            if (arrayValue.type.primary !== DataType.list) {
                arrayValue = Value.list([arrayValue]);
            }
            const elementType = (arrayValue.type as ArrayType).of.prefer(value.type);
            const array = arrayValue.cast(elementType!.asList());
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
            const value = buildValue(left);
            if (value.index && value.index.expressions[0].hash === value.hash) {
                const valueToCompare = buildValue(right);
                if (valueToCompare.isConstant) {
                    const str = valueToCompare.get();
                    if (nullIsh(str)) {
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
                        return new SeqScanFilter(indexed, buildValue(filter));
                    }
                }
            }
        }
    }
    return null;
}

function buildComparison<T>(this: void, on: _ISelection<T>, filter: ExprBinary): _ISelection<T> | null {
    const { op, left, right } = filter;
    let leftValue = buildValue(left);
    let rightValue = buildValue(right);

    if (leftValue.isConstant && rightValue.isConstant) {
        const global = buildValue(filter);
        const got = global.get();
        if (got) {
            return on;
        }
        return new FalseFilter(on);
    }

    if (rightValue.isConstant) {
        rightValue = rightValue.cast(leftValue.type);
    } else if (leftValue.isConstant) {
        leftValue = leftValue.cast(rightValue.type);
    }

    switch (op) {
        case '=':
        case '!=': {
            if (leftValue.index && rightValue.isConstant) {
                return new EqFilter(leftValue, rightValue.get(), op === '=' ? 'eq' : 'neq', false)
            }
            if (rightValue.index && leftValue.isConstant) {
                return new EqFilter(rightValue, leftValue.get(), op === '=' ? 'eq' : 'neq', false);
            }
            break;
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
            break;
    }
    return null;
}

function buildTernaryFilter<T>(this: void, on: _ISelection<T>, filter: ExprTernary): _ISelection<T> | null {
    switch (filter.op) {
        case 'BETWEEN':
        case 'NOT BETWEEN': {
            const value = buildValue(filter.value);
            const lo = buildValue(filter.lo);
            const hi = buildValue(filter.hi);
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
    return null;
}