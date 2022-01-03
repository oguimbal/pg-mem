import { _ISchema, QueryError } from '../../interfaces-private';
import { numbers, isInteger, dateTypes, Types } from '../../datatypes';
import { intervalToSec, queryJson } from '../../utils';
import moment from 'moment';

export function registerCommonOperators(schema: _ISchema) {
    registerNumericOperators(schema);
    registerDatetimeOperators(schema);
    registerJsonOperators(schema);
    registerTextOperators(schema);
}



function registerNumericOperators(schema: _ISchema) {
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
}


function registerDatetimeOperators(schema: _ISchema) {
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



function registerJsonOperators(schema: _ISchema) {
    // ======= "json @> json" query operator
    schema.registerOperator({
        operator: '@>',
        left: Types.jsonb,
        right: Types.jsonb,
        returns: Types.bool,
        implementation: (a, b) => queryJson(b, a),
    });

    // ======= "json - text" (remove key)
    schema.registerOperator({
        operator: '-',
        left: Types.jsonb,
        right: Types.text(),
        returns: Types.jsonb,
        implementation: (a, b) => {
            if (Array.isArray(a)) {
                return a.filter(x => x !== b);
            }
            if (typeof a === 'object') {
                const ret = { ...a };
                delete ret[b];
                return ret;
            }
            throw new QueryError('cannot delete from scalar', '22023');
        },
    });

    // ======= "json - int" (remove index)
    schema.registerOperator({
        operator: '-',
        left: Types.jsonb,
        right: Types.integer,
        returns: Types.jsonb,
        implementation: (a, b) => {
            if (Array.isArray(a)) {
                const ret = [...a];
                ret.splice(b, 1);
                return ret;
            }
            if (typeof a === 'object') {
                throw new QueryError('cannot delete from object using integer index', '22023');
            }
            throw new QueryError('cannot delete from scalar', '22023');
        },
    })
}


function registerTextOperators(schema: _ISchema) {
    // ======== "text || text" (concat text operator)
    schema.registerOperator({
        operator: '||',
        left: Types.text(),
        right: Types.text(),
        returns: Types.text(),
        implementation: (a, b) => a + b,
    })
}
