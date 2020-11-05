export const SCHEMA_NAMESPACE = 11;
export const MAIN_NAMESPACE = 2200;

type OidType = 'table' | 'index';
export function makeOid(type: OidType, id: string) {
    return `oid:${type}:${id}`;
}

export function parseOid(oid: string): { type: OidType; id: string } {
    const [_, type, id] = /^oid:([^:]+):([^:]+)$/.exec(oid) ?? [];
    return {
        type: type as OidType,
        id
    }
}