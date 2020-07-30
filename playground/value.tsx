import React from 'react';
import { nullIsh } from '../src/utils';
import moment from 'moment';

export const ValueDisplay = ({ value, singleLine }: { value: any, singleLine?: boolean }) => {
    if (nullIsh(value)) {
        return (<span className="null">NULL</span>)
    }
    switch (typeof value) {
        case 'number':
            return (<span className="number">{value}</span>);
        case 'string':
            return (<span className="string">{value}</span>);
        case 'boolean':
            return (<span className="bool">{value ? 'true' : 'false'}</span>);
        case 'object':
            if (value instanceof Date) {
                const val = moment(value);
                const repr = Math.abs(moment(val).startOf('day').diff(val)) < 10
                    ? val.format('YYYY-MM-DD')
                    : val.format('YYYY-MM-DD HH:mm:ss');
                return (<span className="date">{repr}</span>);
            }
            return singleLine
                ? (<span className="json">{JSON.stringify(value)}</span>)
                : (<pre className="json">{JSON.stringify(value, null, '    ')}</pre>);
        default:
            return (<span>{JSON.stringify(value)}</span>);

    }
}