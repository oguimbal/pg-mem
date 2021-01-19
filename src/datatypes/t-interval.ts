import { DataType, nil, _IType } from '../interfaces-private';
import { Interval, normalizeInterval, parseIntervalLiteral } from 'pgsql-ast-parser';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../valuetypes';
import { intervalToSec } from '../utils';

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
