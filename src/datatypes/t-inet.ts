import { Evaluator } from '../evaluator';
import { TypeBase } from './datatype-base';
import { DataType, nil, QueryError } from '../interfaces';
import { _ISchema, _IType } from '../interfaces-private';

export class INetType extends TypeBase<string> {

    get primary(): DataType {
        return DataType.inet
    }

    doCanCast(to: _IType) {
        return to.primary === DataType.text;
    }

    doCast(value: Evaluator<string>, to: _IType<string>): Evaluator<any> | nil {
        return value;
    }

    prefer(type: _IType<any>): _IType | nil {
        return this;
    }

    doCanBuildFrom(from: _IType): boolean | nil {
        return from.primary === DataType.text;
    }

    doBuildFrom(value: Evaluator<string>, from: _IType<string>): Evaluator<string> | nil {
        return value
            .setConversion(x => {
                const [_, a, b, c, d, __, m] = /^(\d+)\.(\d+)\.(\d+)\.(\d+)(\/(\d+))?$/.exec(x) ?? []
                if ([a, b, c, d].some(notByte) || notMask(m)) {
                    throw new QueryError(`invalid input syntax for type inet: ${x}`);
                }
                return x;
            }, toInet => ({ toInet }));
    }
}

function notByte(b: string) {
    return !b
        || b.length > 1 && b[0] === '0'
        || parseInt(b, 10) > 255;
}

function notMask(b: string) {
    return b
        && (b.length > 1 && b[0] === '0'
            || parseInt(b, 10) > 32);
}