import { Types } from '../datatypes';
import { DataType, FieldInfo, IBoundQuery, IPreparedQuery, QueryDescription, QueryResult } from '../interfaces';
import { _IType } from '../interfaces-private';
import { nullIsh } from '../utils';

export class InterceptedPreparedQuery implements IPreparedQuery, IBoundQuery {
    constructor(private command: string, private result: any[]) {
    }

    describe(): QueryDescription {
        const template = this.result[0];
        const keys = Object.keys(template);
        const fields = keys.map<FieldInfo>((name, index) => ({
            name,
            type: DataType.text,
            typeId: Types.text().reg.typeId,
            index,
        }));
        const fieldsByKey = Object.fromEntries(fields.map(x => [x.name, x]));
        for (const k of keys) {
            for (const v of this.result) {
                if (nullIsh(v[k])) {
                    continue;
                }
                const t = inferType(v[k]);
                if (t) {
                    fieldsByKey[k].type = t.primary;
                    fieldsByKey[k].typeId = t.reg.typeId;
                    break;
                }
            }
        }

        return {
            parameters: [{ type: DataType.text, typeId: Types.text().reg.typeId }],
            result: fields,
        };
    }

    bind(args?: any[]): IBoundQuery {
        return this;
    }

    *iterate(): IterableIterator<QueryResult> {
        yield this.executeAll();
    }

    executeAll(): QueryResult {
        return {
            command: this.command,
            fields: [],
            location: { start: 0, end: this.command.length },
            rowCount: 0,
            rows: this.result,
        };
    }




}


function inferType(v: any): _IType | null {
    switch (typeof v) {
        case 'bigint':
            return Types.bigint;
        case 'number':
            if (Number.isInteger(v)) {
                return Types.integer;
            }
            return Types.float;
        case 'string':
            return Types.text();
        case 'boolean':
            return Types.bool;
        case 'object':
            if (v instanceof Date) {
                return Types.timestamp();
            }
            return Types.jsonb;
        default:
            return null;
    }
}
