import { DataType, nil, _IType } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../evaluator';
import { deepCompare, deepEqual } from '../utils';
import { Types } from './datatypes';

const NIL = Symbol('null');


export function cleanResults(results: any[]): any {
    // ugly hack to turn jsonb nulls into actual nulls
    // This will bite me someday ... but please dont judge me, I too try to have a life outside here 🤔
    // The sane thing to do would be to refactor things & introduce a DBNULL value in pgmem
    //   since the need of such DBNULL value could arise somehow on another type
    //   see `can select jsonb null` test in nulls.spec.ts

    for (const obj of results) {
        for (const [k, v] of Object.entries(obj)) {
            if (v === NIL) {
                obj[k] = null;
            }
        }
    }
    return results;
}

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
                    .setConversion(raw => JSON.parse(raw, (_, x) => x ?? NIL) ?? NIL
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
