import 'mocha';
import 'chai';
import { checkCreateTable } from './spec-utils';

describe('[PG syntax] Create table', () => {

    checkCreateTable(['create table test(value text)'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'value',
            dataType: { type: 'text' },
        }],
    });

    checkCreateTable(['create table if not exists test(value text)'], {
        type: 'create table',
        name: 'test',
        ifNotExists: true,
        columns: [{
            name: 'value',
            dataType: { type: 'text' },
        }],
    });

    checkCreateTable(['create table"test"(value text primary key)'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'value',
            dataType: { type: 'text' },
            constraint: { type: 'primary key' },
        }],
    });


    checkCreateTable(['create table"test"(value text unique)'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'value',
            dataType: { type: 'text' },
            constraint: { type: 'unique' },
        }],
    });


    checkCreateTable(['create table"test"(value text unique not null)'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'value',
            dataType: { type: 'text' },
            constraint: { type: 'unique', notNull: true },
        }],
    });


    checkCreateTable(['create table"test"(value text[])'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'value',
            dataType: { type: 'array', arrayOf: { type: 'text' } },
        }],
    });


    checkCreateTable(['create table"test"(value text[][])'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'value',
            dataType: { type: 'array', arrayOf: { type: 'array', arrayOf: { type: 'text' } } },
        }],
    });


    checkCreateTable(['create table"test"(id"text"primary key, value text unique not null)'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'id',
            dataType: { type: 'text' },
            constraint: { type: 'primary key' },
        }, {
            name: 'value',
            dataType: { type: 'text' },
            constraint: { type: 'unique', notNull: true },
        }],
    });

    checkCreateTable(['create table"test"(id serial not null)'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'id',
            dataType: { type: 'serial' },
            constraint: { type: 'not null' },
        }],
    });



    checkCreateTable(['create table"test"(a text, constraint pkey primary key(a), b text)'], {
        type: 'create table',
        name: 'test',
        columns: [{
            name: 'a',
            dataType: { type: 'text' },
        }, {
            name: 'b',
            dataType: { type: 'text' },
        }],
        constraints: [{
            type: 'primary key',
            constraintName: 'pkey',
            columns: ['a'],
        }]
    });
});