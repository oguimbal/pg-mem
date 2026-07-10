import { _ITable, _ISchema, setId } from '../../interfaces-private';
import { Schema } from '../../interfaces';
import { Types } from '../../datatypes';
import { ReadOnlyTable } from '../readonly-table';
import { toSql } from 'pgsql-ast-parser';

// https://www.postgresql.org/docs/current/view-pg-policies.html
export class PgPoliciesTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_policies',
        fields: [
            { name: 'schemaname', type: Types.text() }
            , { name: 'tablename', type: Types.text() }
            , { name: 'policyname', type: Types.text() }
            , { name: 'permissive', type: Types.text() }
            , { name: 'roles', type: Types.text().asArray() }
            , { name: 'cmd', type: Types.text() }
            , { name: 'qual', type: Types.text() }
            , { name: 'with_check', type: Types.text() }
        ]
    };

    entropy(): number {
        return 0;
    }

    *enumerate() {
        for (const schema of this.db.listSchemas()) {
            for (const table of schema.listTables()) {
                for (const p of table.rls.policies) {
                    const ret = {
                        schemaname: schema.name,
                        tablename: table.name,
                        policyname: p.name,
                        permissive: p.permissive ? 'PERMISSIVE' : 'RESTRICTIVE',
                        // postgres shows {public} when no roles are named
                        roles: p.roles.length ? p.roles : ['public'],
                        cmd: p.command.toUpperCase(),
                        qual: p.using ? toSql.expr(p.using) : null,
                        with_check: p.withCheck ? toSql.expr(p.withCheck) : null,
                    };
                    yield setId(ret, `/schema/${schema.name}/pg_policies/${table.name}/${p.name}`);
                }
            }
        }
    }

    hasItem(value: any): boolean {
        return !!value;
    }
}
