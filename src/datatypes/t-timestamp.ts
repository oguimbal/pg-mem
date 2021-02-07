import { DataType, nil, QueryError, _IType } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../evaluator';
import moment from 'moment';

export class TimestampType extends TypeBase<Date> {


    constructor(readonly primary: DataType) {
        super();
    }

    doCanCast(to: _IType) {
        switch (to.primary) {
            case DataType.timestamp:
            case DataType.date:
            case DataType.time:
                return true;
        }
        return null;
    }

    doCast(value: Evaluator, to: _IType) {
        switch (to.primary) {
            case DataType.timestamp:
                return value;
            case DataType.date:
                return value
                    .setConversion(raw => moment.utc(raw).startOf('day').toDate()
                        , toDate => ({ toDate }));
            case DataType.time:
                return value
                    .setConversion(raw => moment.utc(raw).format('HH:mm:ss') + '.000000'
                        , toDate => ({ toDate }));
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

    doBuildFrom(value: Evaluator, from: _IType): Evaluator<Date> | nil {
        switch (from.primary) {
            case DataType.text:
                switch (this.primary) {
                    case DataType.timestamp:
                        return value
                            .setConversion(str => {
                                const conv = moment.utc(str);
                                if (!conv.isValid()) {
                                    throw new QueryError(`Invalid timestamp format: ` + str);
                                }
                                return conv.toDate()
                            }
                                , toTs => ({ toTs }));
                    case DataType.date:
                        return value
                            .setConversion(str => {
                                const conv = moment.utc(str);
                                if (!conv.isValid()) {
                                    throw new QueryError(`Invalid timestamp format: ` + str);
                                }
                                return conv.startOf('day').toDate();
                            }
                                , toDate => ({ toDate }));
                }
        }
        return null;
    }



    doEquals(a: any, b: any): boolean {
        return moment(a).diff(moment(b)) < 0.1;
    }
    doGt(a: any, b: any): boolean {
        return moment(a).diff(moment(b)) > 0;
    }
    doLt(a: any, b: any): boolean {
        return moment(a).diff(moment(b)) < 0;
    }
}