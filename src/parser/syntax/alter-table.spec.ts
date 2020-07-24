import 'mocha';
import 'chai';
import { checkAlterTable } from './spec-utils';

describe('PG syntax: Alter table', () => {

    checkAlterTable(['alter table test rename to newname'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'rename',
            to: 'newname'
        }
    });

    checkAlterTable(['alter table test rename column a to b', 'alter table test rename a to b',], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'rename column',
            column: 'a',
            to: 'b',
        }
    });

    checkAlterTable(['alter table test rename constraint a to b',], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'rename constraint',
            constraint: 'a',
            to: 'b',
        }
    });

    checkAlterTable(['alter table test add column a jsonb not null', 'alter table test add a jsonb not null'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'add column',
            column: {
                name: 'a',
                dataType: { type: 'jsonb' },
                constraint: { type: 'not null' },
            },
        }
    });

    checkAlterTable(['alter table test add column if not exists a jsonb not null', 'alter table test add if not exists a jsonb not null'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'add column',
            ifNotExists: true,
            column: {
                name: 'a',
                dataType: { type: 'jsonb' },
                constraint: { type: 'not null' },
            },
        }
    });

    checkAlterTable(['alter table test drop column if exists a', 'alter table test drop if exists a'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'drop column',
            column: 'a',
            ifExists: true,
        }
    });

    checkAlterTable(['alter table test drop column a', 'alter table test drop a'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'drop column',
            column: 'a',
        }
    });

    checkAlterTable(['alter table test alter column a set data type jsonb', 'alter table test alter a type jsonb'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'alter column',
            column: 'a',
            alter: {
                type: 'set type',
                dataType: { type: 'jsonb' },
            }
        }
    });
    checkAlterTable(['alter table test alter a set default 42'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'alter column',
            column: 'a',
            alter: {
                type: 'set default',
                default: { type: 'integer', value: 42 }
            }
        }
    });
    checkAlterTable(['alter table test alter a drop default'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'alter column',
            column: 'a',
            alter: {
                type: 'drop default',
            }
        }
    });
    checkAlterTable(['alter table test alter a  drop not null'], {
        type: 'alter table',
        table: { table: 'test' },
        change: {
            type: 'alter column',
            column: 'a',
            alter: {
                type: 'drop not null',
            }
        }
    });


    checkAlterTable(`ALTER TABLE "photo" ADD CONSTRAINT "FK_4494006ff358f754d07df5ccc87"
                 FOREIGN KEY ("userId")
                REFERENCES "user"("id")
                ON DELETE NO ACTION ON UPDATE NO ACTION;`, {
        type: 'alter table',
        table: { table: 'photo' },
        change: {
            type: 'add constraint',
            constraintName: 'FK_4494006ff358f754d07df5ccc87',
            constraint: {
                type: 'foreign key',
                localColumns: ['userId'],
                foreignTable: 'user',
                foreignColumns: ['id'],
                onUpdate: 'no action',
                onDelete: 'no action',
            }
        }
    })
});