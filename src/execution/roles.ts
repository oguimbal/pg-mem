import { _IStatementExecutor, _Transaction, StatementResult, GLOBAL_VARS, QueryError } from '../interfaces-private';
import { CreateRoleStatement, SetRoleStatement, ResetStatement, DropStatement } from 'pgsql-ast-parser';
import { Map as ImMap } from 'immutable';
import { ExecHelper } from './exec-utils';
import { ignore } from '../utils';

// Roles & session identity.
//
// Roles are stored in the transaction data (rollback-safe) under ROLES. The session's
// current & session roles are GUCs in GLOBAL_VARS. The default role is a synthetic
// superuser (pg_mem), so a database with no roles declared behaves exactly as before
// RLS existed: current_user is 'pg_mem' and RLS is always bypassed.

export const ROLES = Symbol('roles');
const SESSION_ROLE_KEY = 'session_authorization';
const CURRENT_ROLE_KEY = 'role';

export const DEFAULT_ROLE_NAME = 'pg_mem';

export interface Role {
    name: string;
    superuser: boolean;
    login: boolean;
    bypassRls: boolean;
}

const DEFAULT_ROLE: Role = {
    name: DEFAULT_ROLE_NAME,
    superuser: true,
    login: true,
    bypassRls: true,
};

function rolesMap(t: _Transaction): ImMap<string, Role> {
    return t.getMap(ROLES);
}

export function getRole(t: _Transaction, name: string): Role | undefined {
    if (name === DEFAULT_ROLE_NAME) {
        return rolesMap(t).get(name) ?? DEFAULT_ROLE;
    }
    return rolesMap(t).get(name);
}

export function sessionRoleName(t: _Transaction): string {
    return t.getMap(GLOBAL_VARS).get(SESSION_ROLE_KEY) ?? DEFAULT_ROLE_NAME;
}

export function currentRoleName(t: _Transaction): string {
    return t.getMap(GLOBAL_VARS).get(CURRENT_ROLE_KEY) ?? sessionRoleName(t);
}

/** The effective role a query runs as (falls back to the synthetic superuser) */
export function currentRole(t: _Transaction): Role {
    return getRole(t, currentRoleName(t)) ?? DEFAULT_ROLE;
}

function setGuc(t: _Transaction, key: string, value: string | undefined): void {
    let g = t.getMap(GLOBAL_VARS);
    g = value === undefined ? g.delete(key) : g.set(key, value);
    t.set(GLOBAL_VARS, g);
}

export class CreateRoleExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(private stmt: CreateRoleStatement) {
        super(stmt);
    }

    execute(t: _Transaction): StatementResult {
        const name = this.stmt.name.name;
        if (getRole(t, name)) {
            throw new QueryError(`role "${name}" already exists`, '42710');
        }
        const o = this.stmt.options;
        const role: Role = {
            name,
            superuser: o.superuser ?? false,
            login: o.login ?? false,
            bypassRls: o.bypassRls ?? false,
        };
        t.set(ROLES, rolesMap(t).set(name, role));
        return this.noData(t, 'CREATE ROLE');
    }
}

export class DropRoleExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(private stmt: DropStatement) {
        super(stmt);
    }

    execute(t: _Transaction): StatementResult {
        let map = rolesMap(t);
        for (const n of this.stmt.names) {
            const name = n.name;
            if (!map.has(name) && name !== DEFAULT_ROLE_NAME) {
                if (this.stmt.ifExists) {
                    continue;
                }
                throw new QueryError(`role "${name}" does not exist`, '42704');
            }
            map = map.delete(name);
        }
        t.set(ROLES, map);
        return this.noData(t, 'DROP ROLE');
    }
}

export class SetRoleExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(private stmt: SetRoleStatement) {
        super(stmt);
    }

    execute(t: _Transaction): StatementResult {
        ignore(this.stmt.scope); // SET LOCAL vs SESSION treated alike in v1
        if (!this.stmt.role) {
            // SET ROLE NONE -> back to the session role
            setGuc(t, CURRENT_ROLE_KEY, undefined);
            return this.noData(t, 'SET');
        }
        const name = this.stmt.role.name;
        if (!getRole(t, name)) {
            throw new QueryError(`role "${name}" does not exist`, '42704');
        }
        setGuc(t, CURRENT_ROLE_KEY, name);
        return this.noData(t, 'SET');
    }
}

export class ResetExecutor extends ExecHelper implements _IStatementExecutor {
    constructor(private stmt: ResetStatement) {
        super(stmt);
    }

    execute(t: _Transaction): StatementResult {
        const id = this.stmt.identifier;
        if (id === 'all') {
            setGuc(t, CURRENT_ROLE_KEY, undefined);
        } else if (id.name.toLowerCase() === 'role') {
            setGuc(t, CURRENT_ROLE_KEY, undefined);
        } else {
            // other RESET <param> -> clear that GUC
            setGuc(t, id.name, undefined);
        }
        return this.noData(t, 'RESET');
    }
}
