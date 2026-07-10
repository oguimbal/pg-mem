import { _ITable, _Transaction, setId } from '../../interfaces-private.ts';
import { Schema } from '../../interfaces.ts';
import { Types } from '../../datatypes/index.ts';
import { ReadOnlyTable } from '../readonly-table.ts';
import { listRoles } from '../../execution/roles.ts';

// https://www.postgresql.org/docs/current/view-pg-roles.html
// Minimal projection of the columns migrations/tools actually read (rolname + the
// common boolean attributes). Roles are transaction-scoped (see roles.ts).
export class PgRolesTable extends ReadOnlyTable implements _ITable {

    _schema: Schema = {
        name: 'pg_roles',
        fields: [
            { name: 'rolname', type: Types.text() }
            , { name: 'rolsuper', type: Types.bool }
            , { name: 'rolinherit', type: Types.bool }
            , { name: 'rolcreaterole', type: Types.bool }
            , { name: 'rolcreatedb', type: Types.bool }
            , { name: 'rolcanlogin', type: Types.bool }
            , { name: 'rolreplication', type: Types.bool }
            , { name: 'rolbypassrls', type: Types.bool }
            , { name: 'rolconnlimit', type: Types.integer }
            , { name: 'rolvaliduntil', type: Types.timestamptz() }
        ]
    };

    entropy(): number {
        return 0;
    }

    *enumerate(t: _Transaction) {
        for (const r of listRoles(t)) {
            const ret = {
                rolname: r.name,
                rolsuper: r.superuser,
                rolinherit: true,
                rolcreaterole: r.superuser,
                rolcreatedb: r.superuser,
                rolcanlogin: r.login,
                rolreplication: false,
                rolbypassrls: r.bypassRls,
                rolconnlimit: -1,
                rolvaliduntil: null,
            };
            yield setId(ret, `/pg_roles/${r.name}`);
        }
    }

    hasItem(value: any): boolean {
        return !!value;
    }
}
