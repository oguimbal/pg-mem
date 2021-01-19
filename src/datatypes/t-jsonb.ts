import { DataType, nil, QueryError, _IType } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../valuetypes';
import { deepCompare, deepEqual } from '../utils';
import { Types } from './datatypes';


export class JSONBType extends TypeBase<any> {


    constructor(readonly primary: DataType) {
        super();
    }

    doCanCast(_to: _IType): boolean | nil {
        switch (_to.primary) {
            case DataType.text:
            case DataType.json:
            case DataType.jsonb:
                return true;
        }
        return null;
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        if (to.primary === DataType.json) {
            return a
                .setType(Types.text())
                .setConversion(json => JSON.stringify(json)
                    , toJsonB => ({ toJsonB }))
                .convert(to) as Evaluator; // <== might need truncation
        }

        // json
        return a.setType(to);
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
                return value
                    .setConversion(raw => JSON.parse(raw)
                        , toJsonb => ({ toJsonb }));
        }
        return null;
    }



    doEquals(a: any, b: any): boolean {
        return deepEqual(a, b, false);
    }

    doGt(a: any, b: any): boolean {
        return deepCompare(a, b) > 0;
    }

    doLt(a: any, b: any): boolean {
        return deepCompare(a, b) < 0;
    }
}
