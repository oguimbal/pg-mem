import { DataType, nil, _IType } from '../interfaces-private.ts';
import { Interval, normalizeInterval, parseIntervalLiteral } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../evaluator.ts';
import { intervalToSec } from '../utils.ts';

export class IntervalType extends TypeBase<Interval> {

    get primary(): DataType {
        return DataType.interval;
    }

    doCanBuildFrom(from: _IType) {
        switch (from.primary) {
            case DataType.text:
                return true;
        }
        return false;
    }

    doBuildFrom(value: Evaluator, from: _IType): Evaluator<Interval> | nil {
        switch (from.primary) {
            case DataType.text:
                return value
                    .setConversion(str => {
                        const conv = normalizeInterval(parseIntervalLiteral(str));
                        return conv;
                    }
                        , toInterval => ({ toInterval }));
        }
        return null;
    }

    doEquals(a: Interval, b: Interval): boolean {
        return intervalToSec(a) === intervalToSec(b);
    }
    doGt(a: Interval, b: Interval): boolean {
        return intervalToSec(a) > intervalToSec(b);
    }
    doLt(a: Interval, b: Interval): boolean {
        return intervalToSec(a) < intervalToSec(b);
    }
}
