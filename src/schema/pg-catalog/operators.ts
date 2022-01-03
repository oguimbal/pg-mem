import { _ISchema } from '../../interfaces-private';
import { numbers, isInteger, dateTypes, Types } from '../../datatypes';
import { intervalToSec } from '../../utils';
import moment from 'moment';

export function registerCommonOperators(schema: _ISchema) {

    // ======= "+ - * /" on numeric types =======
    for (const t of numbers) {
        schema.registerOperator({
            operator: '+',
            commutative: true,
            left: t,
            right: t,
            returns: t,
            implementation: (a, b) => a + b,
        });
    }
    for (const t of numbers) {
        schema.registerOperator({
            operator: '-',
            commutative: true,
            left: t,
            right: t,
            returns: t,
            implementation: (a, b) => a - b,
        });
    }

    for (const t of numbers) {
        schema.registerOperator({
            operator: '*',
            commutative: true,
            left: t,
            right: t,
            returns: t,
            implementation: (a, b) => a * b,
        });
    }
    for (const t of numbers) {
        schema.registerOperator({
            operator: '/',
            commutative: false,
            left: t,
            right: t,
            returns: t,
            implementation: isInteger(t)
                ? ((a, b) => Math.trunc(a / b))
                : ((a, b) => a / b),
        });
    }

    // ======= date/time "+ -" timestamp =======
    for (const dt of dateTypes) {
        for (const [operator, f] of [['+', 1], ['-', -1]] as const) {
            schema.registerOperator({
                operator,
                commutative: operator === '+',
                left: dt,
                right: Types.interval,
                returns: dt,
                implementation: (a, b) => moment(a).add(f * intervalToSec(b), 'seconds').toDate(),
            });
        }
    }

    // ==== date "+ -" integer  (add days.. only works on dates, not timestamps)
    for (const [operator, f] of [['+', 1], ['-', -1]] as const) {
        schema.registerOperator({
            operator,
            commutative: operator === '+',
            left: Types.date,
            right: Types.integer,
            returns: Types.date,
            implementation: (a, b) => moment(a).add(f * b, 'days').toDate(),
        });
    }
}
