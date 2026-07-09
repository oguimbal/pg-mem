import { DataType, nil, _IType, _ISchema, _Transaction, _IRelation } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { RecordCol } from './datatypes';
import { Evaluator } from '../evaluator';
import { nullIsh } from '../utils';
import hash from 'object-hash';

export function asComposite(o: _IRelation | null): CompositeType | null {
    return o && o.type === 'type' && o instanceof CompositeType ? o : null;
}

/** A named composite type: `CREATE TYPE x AS (a int, b text)`. A value is an object keyed
 * by field name; it is built from a record/row by mapping fields positionally. */
export class CompositeType extends TypeBase<any> {

    get primary(): DataType {
        return this._name as any; // registered/identified under its own name (like an enum)
    }

    get name(): string {
        return this._name;
    }

    constructor(
        readonly schema: _ISchema,
        private readonly _name: string,
        readonly columns: RecordCol[],
    ) {
        super(null);
    }

    install(): this {
        this.schema._registerType(this);
        return this;
    }

    drop(t: _Transaction): void {
        this.schema._unregisterType(this);
    }

    doEquals(a: any, b: any): boolean {
        if (nullIsh(a) || nullIsh(b)) { return a === b; }
        return this.columns.every(c => c.type.equals(a[c.name], b[c.name]));
    }

    // built from a record/row (e.g. ROW(1,'a')::mytype): map fields positionally
    doCanBuildFrom(from: _IType): boolean | nil {
        return from.primary === DataType.record || from instanceof CompositeType;
    }

    doBuildFrom(value: Evaluator, from: _IType): Evaluator | nil {
        const fromCols = (from as any).columns as RecordCol[] | undefined;
        const target = this.columns;
        return new Evaluator(
            this,
            value.id,
            hash({ composite: this._name, v: value.hash }),
            value,
            (raw, t) => {
                const v = value.get(raw, t);
                if (nullIsh(v)) { return null; }
                const keys = fromCols ? fromCols.map(c => c.name) : Object.keys(v);
                const ret: any = {};
                target.forEach((c, i) => ret[c.name] = v[keys[i]] ?? null);
                return ret;
            },
        );
    }
}
