import { _ISchema } from '../../interfaces-private.ts';
import { Types } from '../../datatypes/index.ts';
import {
    RangeType, RANGE_ADAPTERS, parseRangeLiteral, canonicalize,
    rangeContainsRange, rangeContainsElem, rangesOverlap,
} from '../../datatypes/t-range';

/**
 * Registers the built-in range types (int4range, int8range, numrange, daterange,
 * tsrange, tstzrange) together with their constructors, containment/overlap operators
 * and lower()/upper()/isempty()/lower_inc()/upper_inc() accessors.
 */
export function registerRanges(schema: _ISchema) {
    for (const adapter of RANGE_ADAPTERS) {
        const range = new RangeType(schema, adapter).install();
        const elem = adapter.elem();

        // ---- constructors: xrange(lo, hi) and xrange(lo, hi, bounds) ----
        const build = (lo: any, hi: any, bounds: string) => canonicalize(adapter, {
            empty: false,
            loInc: bounds[0] !== '(',
            hiInc: bounds[1] === ']',
            lo: lo == null ? null : adapter.fromRaw(lo),
            hi: hi == null ? null : adapter.fromRaw(hi),
        });
        schema.registerFunction({
            name: adapter.name,
            args: [elem, elem],
            returns: range,
            allowNullArguments: true,
            implementation: (lo, hi) => build(lo, hi, '[)'),
        });
        schema.registerFunction({
            name: adapter.name,
            args: [elem, elem, Types.text()],
            returns: range,
            allowNullArguments: true,
            implementation: (lo, hi, bounds) => build(lo, hi, bounds ?? '[)'),
        });

        // ---- operators ----
        // range @> range   (contains)
        schema.registerOperator({
            operator: '@>', left: range, right: range, returns: Types.bool,
            implementation: (a, b) => rangeContainsRange(adapter, a, b),
        });
        // range @> element (contains element)
        schema.registerOperator({
            operator: '@>', left: range, right: elem, returns: Types.bool,
            implementation: (r, e) => rangeContainsElem(adapter, r, adapter.key(adapter.fromRaw(e))),
        });
        // element <@ range
        schema.registerOperator({
            operator: '<@', left: elem, right: range, returns: Types.bool,
            implementation: (e, r) => rangeContainsElem(adapter, r, adapter.key(adapter.fromRaw(e))),
        });
        // range <@ range   (contained by)
        schema.registerOperator({
            operator: '<@', left: range, right: range, returns: Types.bool,
            implementation: (a, b) => rangeContainsRange(adapter, b, a),
        });
        // range && range   (overlaps)
        schema.registerOperator({
            operator: '&&', left: range, right: range, returns: Types.bool, commutative: true,
            implementation: (a, b) => rangesOverlap(adapter, a, b),
        });

        // ---- accessors ----
        schema.registerFunction({
            name: 'lower', args: [range], returns: elem, allowNullArguments: true,
            implementation: r => {
                if (r == null) { return null; }
                const b = parseRangeLiteral(r);
                return b.empty || b.lo === null ? null : adapter.toRaw(b.lo);
            },
        });
        schema.registerFunction({
            name: 'upper', args: [range], returns: elem, allowNullArguments: true,
            implementation: r => {
                if (r == null) { return null; }
                const b = parseRangeLiteral(r);
                return b.empty || b.hi === null ? null : adapter.toRaw(b.hi);
            },
        });
        schema.registerFunction({
            name: 'isempty', args: [range], returns: Types.bool, allowNullArguments: true,
            implementation: r => r == null ? null : parseRangeLiteral(r).empty,
        });
        schema.registerFunction({
            name: 'lower_inc', args: [range], returns: Types.bool, allowNullArguments: true,
            implementation: r => {
                if (r == null) { return null; }
                const b = parseRangeLiteral(r);
                return !b.empty && b.lo !== null && b.loInc;
            },
        });
        schema.registerFunction({
            name: 'upper_inc', args: [range], returns: Types.bool, allowNullArguments: true,
            implementation: r => {
                if (r == null) { return null; }
                const b = parseRangeLiteral(r);
                return !b.empty && b.hi !== null && b.hiInc;
            },
        });
    }
}
