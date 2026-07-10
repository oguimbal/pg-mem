import { DataType, nil, _IType, _ISchema } from '../interfaces-private';
import { TypeBase } from './datatype-base';
import { Evaluator } from '../evaluator';
import { nullIsh } from '../utils';
import { parseTsvectorLiteral, parseTsqueryLiteral } from '../functions/text-search';

/** tsvector / tsquery: named types stored as their canonical text form. */
export class TextSearchType extends TypeBase<string> {

    get primary(): DataType {
        return this._name as any;
    }

    get name(): string {
        return this._name;
    }

    constructor(
        readonly schema: _ISchema,
        private readonly _name: 'tsvector' | 'tsquery',
        private readonly normalize: (t: string) => string,
    ) {
        super(null);
    }

    install(): this {
        this.schema._registerType(this);
        return this;
    }

    doCanCast(to: _IType): boolean | nil {
        return to.primary === DataType.text;
    }

    doCast(a: Evaluator, to: _IType): Evaluator {
        return a.setType(to);
    }

    doCanBuildFrom(from: _IType): boolean | nil {
        return from.primary === DataType.text;
    }

    doBuildFrom(value: Evaluator, from: _IType): Evaluator | nil {
        if (from.primary !== DataType.text) { return null; }
        const norm = this.normalize;
        return value.setConversion(
            (raw: string) => nullIsh(raw) ? null : norm(raw),
            toTs => ({ toTs: this._name, v: toTs }),
        ).setType(this);
    }

    doEquals(a: string, b: string): boolean {
        return a === b;
    }
}

export function registerTextSearchTypes(schema: _ISchema): { tsvector: TextSearchType, tsquery: TextSearchType } {
    const tsvector = new TextSearchType(schema, 'tsvector', parseTsvectorLiteral).install();
    const tsquery = new TextSearchType(schema, 'tsquery', parseTsqueryLiteral).install();
    return { tsvector, tsquery };
}
