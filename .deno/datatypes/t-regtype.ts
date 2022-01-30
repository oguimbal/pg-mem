import { DataType, nil, QueryError, RegClass, RegType, _IType } from '../interfaces-private.ts';
import { TypeBase } from './datatype-base.ts';
import { Evaluator } from '../evaluator.ts';
import { Types } from './datatypes.ts';
import { buildCtx } from '../parser/context.ts';

export class RegTypeImpl extends TypeBase<RegType> {


    get primary(): DataType {
        return DataType.regtype;
    }

    doCanCast(_to: _IType): boolean | nil {
        switch (_to.primary) {
            case DataType.text:
            case DataType.integer:
                return true;
        }
        return null;
    }

    doCast(a: Evaluator<RegType>, to: _IType): Evaluator {
        switch (to.primary) {
            case DataType.text:
                return a
                    .setType(to)
                    .setConversion(raw => raw.toString(10)
                        , toText => ({ toText }))
            case DataType.integer:
                const { schema } = buildCtx();
                return a
                    .setType(to)
                    .setConversion((raw: RegType) => {
                        if (typeof raw === 'number') {
                            return raw;
                        }
                        const t = schema.parseType(raw);
                        return t.reg.typeId;
                    }
                        , toText => ({ toText }))
        }
        throw new Error('failed to cast');
    }


    doCanBuildFrom(from: _IType) {
        switch (from.primary) {
            case DataType.text:
                return true;
        }
        return false;
    }

    doBuildFrom(value: Evaluator, from: _IType): Evaluator<RegClass> | nil {
        switch (from.primary) {
            case DataType.text:
                const { schema } = buildCtx();
                return value
                    .setType(Types.regtype)
                    .setConversion((str: string) => {
                        let repl = str.replace(/["\s]+/g, '');
                        if (repl.startsWith('pg_catalog.')) {
                            repl = repl.substr('pg_catalog.'.length);
                        }
                        return schema.parseType(repl).name;
                    }
                        , strToRegType => ({ strToRegType }));
        }
        return null;
    }

}
