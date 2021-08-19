import { DataType, nil, _IType } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../evaluator';
import { deepCompare, deepEqual } from '../utils';
import { Types } from './datatypes';
import { JSON_NIL } from '../clean-results';
import { QueryError } from '../interfaces';

export class JSONBType extends TypeBase<any> {


    constructor(readonly primary: DataType) {
        super();
    }

    doCanCast(_to: _IType): boolean | nil {
        switch (_to.primary) {
            case DataType.text:
            case DataType.json:
            case DataType.jsonb:
            case DataType.float:
            case DataType.bool:
            case DataType.integer:
                return true;
        }
        return null;
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        switch (to.primary) {
            case DataType.json:
                return a
                    .setType(Types.text())
                    .setConversion(json => JSON.stringify(this.toResult(json))
                        , toJsonB => ({ toJsonB }))
                    .convert(to) as Evaluator; // <== might need truncation
            case DataType.jsonb:
                return a.setType(to);
            case DataType.float:
            case DataType.integer:
                const isInt = to.primary === DataType.integer;
                return a
                    .setType(to)
                    .setConversion(json => {
                        if (typeof json !== 'number') {
                            throw new QueryError('cannot cast jsonb string to type ' + isInt ? 'integer' : 'double precision', '22023');
                        }
                        return isInt ? Math.round(json) : json;
                    }, toFloat => ({ toFloat }));
            case DataType.bool:
                return a
                    .setType(to)
                    .setConversion(json => {
                        if (typeof json !== 'boolean') {
                            throw new QueryError('cannot cast jsonb string to type boolean', '22023');
                        }
                        return json;
                    }, toFloat => ({ toFloat }));
            default:
                return a.setType(to);
        }

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
                    .setConversion(raw => {
                        try {
                            return JSON.parse(raw, (_, x) => x ?? JSON_NIL) ?? JSON_NIL
                        } catch (e) {
                            throw new QueryError({
                                error: `invalid input syntax for type json`,
                                details: e.message,
                                code: '22P02',
                            });
                        }
                    }
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
        return result === JSON_NIL
            ? null
            : result;
    }

}
