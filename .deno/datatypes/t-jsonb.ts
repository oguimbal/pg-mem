import { DataType, nil, _IType } from '../interfaces-private.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../evaluator.ts';
import { deepCompare, deepEqual } from '../utils.ts';
import { Types } from './datatypes.ts';

const NIL = Symbol('null');
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
                .setConversion(json => JSON.stringify(this.toResult(json))
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
                    .setConversion(raw => JSON.parse(raw) ?? NIL
                        , toJsonb => ({ toJsonb }));
        }
        return null;
    }



    doEquals(a: any, b: any): boolean {
        return deepEqual(this.toResult(a), this.toResult(b), false);
    }

    doGt(a: any, b: any): boolean {
        return deepCompare(this.toResult(a), this.toResult(b)) > 0;
    }

    doLt(a: any, b: any): boolean {
        return deepCompare(this.toResult(a), this.toResult(b)) < 0;
    }

    toResult(result: any): any {
        return result === NIL
            ? null
            : result;
    }

}
