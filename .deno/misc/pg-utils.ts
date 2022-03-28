// ====== ALMOST A COPY-PASTE OF https://github.com/brianc/node-postgres/blob/4b229275cfe41ca17b7d69bd39f91ada0068a5d0/packages/pg/lib/utils.js#L71-L82
//   see https://github.com/oguimbal/pg-mem/issues/181

import { nullIsh } from '../utils.ts';
import { bufToString, isBuf } from './buffer-deno.ts';
import { literal } from './pg-escape.ts';
import { jsonStringify as  stringify} from 'https://deno.land/x/stable_stringify@v0.2.1/jsonStringify.ts';

export function toLiteral(val: any) {
  return prepareValue(val);
}

// converts values from javascript types
// to their 'raw' counterparts for use as a postgres parameter
// note: you can override this function to provide your own conversion mechanism
// for complex types, etc...
var prepareValue = function (val: any, seen?: any[]): any {
  // null and undefined are both null for postgres
  if (nullIsh(val)) {
    return 'null';
  }

  if (isBuf(val)) {
    return literal(bufToString(val));
  }
  if (val instanceof Date) {
    // if (defaults.parseInputDatesAsUTC) {
    //   return dateToStringUTC(val)
    // } else {
    return literal(dateToString(val));
    // }
  }

  if (Array.isArray(val)) {
    if (val.length === 0) return `'{}'`;
    return `ARRAY[${val.map(x => toLiteral(x)).join(', ')}]`;
  }
  if (typeof val === 'object') {
    return prepareObject(val, seen);
  }
  return literal(val.toString());
}

function prepareObject(val: any, seen?: any[]) {
  if (val && typeof val.toPostgres === 'function') {
    seen = seen || []
    if (seen.indexOf(val) !== -1) {
      throw new Error('circular reference detected while preparing "' + val + '" for query')
    }
    seen.push(val)

    return prepareValue(val.toPostgres(prepareValue), seen)
  }
  return literal(stringify(val));
}

function pad(number: any, digits: number) {
  number = '' + number
  while (number.length < digits) {
    number = '0' + number
  }
  return number
}

function dateToString(date: Date) {
  var offset = -date.getTimezoneOffset()

  var year = date.getFullYear()
  var isBCYear = year < 1
  if (isBCYear) year = Math.abs(year) + 1 // negative years are 1 off their BC representation

  var ret =
    pad(year, 4) +
    '-' +
    pad(date.getMonth() + 1, 2) +
    '-' +
    pad(date.getDate(), 2) +
    'T' +
    pad(date.getHours(), 2) +
    ':' +
    pad(date.getMinutes(), 2) +
    ':' +
    pad(date.getSeconds(), 2) +
    '.' +
    pad(date.getMilliseconds(), 3)

  if (offset < 0) {
    ret += '-'
    offset *= -1
  } else {
    ret += '+'
  }

  ret += pad(Math.floor(offset / 60), 2) + ':' + pad(offset % 60, 2)
  if (isBCYear) ret += ' BC'
  return ret
}

function dateToStringUTC(date: Date) {
  var year = date.getUTCFullYear()
  var isBCYear = year < 1
  if (isBCYear) year = Math.abs(year) + 1 // negative years are 1 off their BC representation

  var ret =
    pad(year, 4) +
    '-' +
    pad(date.getUTCMonth() + 1, 2) +
    '-' +
    pad(date.getUTCDate(), 2) +
    'T' +
    pad(date.getUTCHours(), 2) +
    ':' +
    pad(date.getUTCMinutes(), 2) +
    ':' +
    pad(date.getUTCSeconds(), 2) +
    '.' +
    pad(date.getUTCMilliseconds(), 3)

  ret += '+00:00'
  if (isBCYear) ret += ' BC'
  return ret
}

export function normalizeQueryConfig(config: any, values: any, callback: any) {
  // can take in strings or config objects
  config = typeof config === 'string' ? { text: config } : config
  if (values) {
    if (typeof values === 'function') {
      config.callback = values
    } else {
      config.values = values
    }
  }
  if (callback) {
    config.callback = callback
  }
  return config
}
