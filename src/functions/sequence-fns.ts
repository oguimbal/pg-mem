import { executionCtx } from '../utils';
import { Types } from '../datatypes';
import { FunctionDefinition, QueryError } from '../interfaces';
import { asSeq, RegClass, _ISequence } from '../interfaces-private';

// https://www.postgresql.org/docs/8.1/functions-sequence.html

function getSeq(id: RegClass) {
    const { transaction, schema } = executionCtx();
    if (!transaction) {
        throw new QueryError('cannot query sequence value in this context');
    }
    return {
        t: transaction,
        seq: asSeq(schema.getObjectByRegOrName(id)),
    };
}
const lastVal = Symbol();
export const sequenceFunctions: FunctionDefinition[] = [
    {
        name: 'nextval',
        args: [Types.regclass],
        returns: Types.integer,
        implementation: (seqId: RegClass) => {
            const { seq, t } = getSeq(seqId);
            const ret = seq.nextValue(t);
            t.set(lastVal, ret);
            return ret;
        },
        impure: true,
    },
    {
        name: 'currval',
        args: [Types.regclass],
        returns: Types.integer,
        implementation: (seqId: RegClass) => {
            const { seq, t } = getSeq(seqId);
            return seq.currentValue(t);
        },
        impure: true,
    },
    {
        name: 'lastval',
        returns: Types.integer,
        implementation: (seqId: RegClass) => {
            const { transaction } = executionCtx();
            if (!transaction) {
                throw new QueryError('cannot query lastval in this context');
            }
            return transaction.get<number>(lastVal);
        },
        impure: true,
    },
    {
        name: 'setval',
        args: [Types.regclass, Types.integer],
        returns: Types.integer,
        implementation: (seqId: RegClass, val: number) => {
            const { seq, t } = getSeq(seqId);
            if (typeof val !== 'number') {
                throw new QueryError('Invalid setval() value');
            }
            seq.setValue(t, val);
            return val;
        },
        impure: true,
    },
];
