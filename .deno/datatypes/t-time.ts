import { DataType, nil, _IType } from '../interfaces-private.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../evaluator.ts';
import { parseTime } from '../utils.ts';


export class TimeType extends TypeBase<string> {


    constructor(readonly primary: DataType.time | DataType.timetz) {
        super();
    }


    get name(): string {
        return this.primary === DataType.timetz
            ? 'time with time zone'
            : 'time without time zone';
    }


    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.text:
            case DataType.timetz:
            case DataType.time:
                return true;
        }
        return null;
    }


    doCanConvertImplicit(to: _IType) {
        switch (to.primary) {
            case DataType.timetz:
                return true;
            case DataType.time:
                return this.primary === DataType.time;
        }
        return false;
    }


    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.text:
            case DataType.time:
            case DataType.timetz:
                return value
                    .setType(to);
        }
        throw new Error('Unexpected cast error');
    }


    doCanBuildFrom(from: _IType) {
        switch (from.primary) {
            case DataType.text:
                return true;
        }
        return false;
    }

    doBuildFrom(value: Evaluator, from: _IType): Evaluator<string> | nil {
        switch (from.primary) {
            case DataType.text:
                return value
                    .setConversion(str => {
                        const conv = parseTime(str);
                        const ret = conv.format('HH:mm:ss');
                        const ms = conv.milliseconds();
                        return ms
                            ? ret + (ms / 1000).toString(10).substr(1)
                            : ret;
                    }
                        , toTime => ({ toTime }));
        }
        return null;
    }

}
