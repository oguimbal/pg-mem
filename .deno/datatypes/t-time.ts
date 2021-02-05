import { DataType, nil, QueryError, _IType } from '../interfaces-private.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../valuetypes.ts';
import { Types } from './datatypes.ts';
import { parseTime } from '../utils.ts';


export class TimeType extends TypeBase<string> {


    get primary(): DataType {
        return DataType.time;
    }


    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return true;
        }
        return null;
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.text:
                return value
                    .setType(Types.text())
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
