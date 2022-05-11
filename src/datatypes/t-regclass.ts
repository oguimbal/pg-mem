import { DataType, nil, QueryError, RegClass, _IType } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../evaluator';
import { parseRegClass } from '../utils';
import { Types } from './datatypes';
import { buildCtx } from '../parser/context';

export class RegClassImpl extends TypeBase<RegClass> {
    get primary(): DataType {
        return DataType.regclass;
    }

    doCanCast(_to: _IType): boolean | nil {
        switch (_to.primary) {
            case DataType.text:
            case DataType.integer:
                return true;
        }
        return null;
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        const { schema } = buildCtx();
        switch (to.primary) {
            case DataType.text:
                return a.setType(Types.text()).setConversion(
                    (raw: RegClass) => {
                        return raw?.toString();
                    },
                    (toText) => ({ toText }),
                );
            case DataType.integer:
                return a.setType(Types.text()).setConversion(
                    (raw: RegClass) => {
                        // === regclass -> int

                        const cls = parseRegClass(raw);

                        // if its a number, then try to get it.
                        if (typeof cls === 'number') {
                            return schema.getObjectByRegOrName(cls)?.reg.classId ?? cls;
                        }

                        // get the object or throw
                        return schema.getObjectByRegOrName(raw).reg.classId;
                    },
                    (toText) => ({ toText }),
                );
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
        const { schema } = buildCtx();
        switch (from.primary) {
            case DataType.text:
                return value.setConversion(
                    (str: string) => {
                        // === text -> regclass

                        const cls = parseRegClass(str);

                        // if its a number, then try to get it.
                        if (typeof cls === 'number') {
                            return schema.getObjectByRegOrName(cls)?.name ?? cls;
                        }

                        // else, get or throw.
                        return schema.getObject(cls).name;
                    },
                    (strToRegClass) => ({ strToRegClass }),
                );
        }
        return null;
    }
}
