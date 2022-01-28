import { _Transaction, _ISchema, NotSupported, _ITable, _IStatement, _IStatementExecutor, QueryError, _ArgDefDetails, IType, _IType, nil, FunctionDefinition } from '../../interfaces-private';
import { CreateFunctionStatement } from 'pgsql-ast-parser';
import { resultNoData } from '../exec-utils';
import { buildValue } from '../../parser/expression-builder';
import { Types } from '../../datatypes';
import { ignore } from '../../utils';

export class CreateFunction implements _IStatementExecutor {
    private onSchema: _ISchema;
    private toRegister: FunctionDefinition;
    private replace: boolean;

    constructor({ schema }: _IStatement, private fn: CreateFunctionStatement) {
        if (!fn.language) {
            throw new QueryError('Unspecified function language');
        }
        this.onSchema = schema.getThisOrSiblingFor(fn.name);

        const lang = schema.db.getLanguage(fn.language.name);

        // determine arg types
        const args = fn.arguments.map<_ArgDefDetails>(a => ({
            name: a.name?.name,
            type: schema.getType(a.type),
            default: a.default && buildValue(schema.dualTable.selection, a.default),
            mode: a.mode,
        }));

        // determine return type
        let returns: IType | null = null;
        if (fn.returns) {
            switch (fn.returns.kind) {
                case 'table':
                    // Todo: we're losing the typing here :(
                    returns = Types.record.asArray();
                    ignore(fn.returns.columns);
                    break;
                case 'array':
                case null:
                case undefined:
                    returns = schema.getType(fn.returns);
                    break;
                default:
                    throw NotSupported.never(fn.returns);
            }
        }

        let argsVariadic: _IType | nil;
        const variad = args.filter(x => x.mode === 'variadic');
        if (variad.length > 1) {
            throw new QueryError(`Expected only one "VARIADIC" argument`);
        } else if (variad.length) {
            argsVariadic = variad[0].type;
        }

        // compile & register the associated function
        const compiled = lang({
            args,
            code: fn.code,
            returns,
            functioName: fn.name.name,
            schema: schema,
        });

        this.toRegister = {
            name: fn.name.name,
            returns,
            implementation: compiled,
            args: args.filter(x => x.mode !== 'variadic'),
            argsVariadic,
            impure: fn.purity !== 'immutable',
            allowNullArguments: fn.onNullInput === 'call',
        };
        this.replace = fn.orReplace ?? false;
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        this.onSchema.registerFunction(this.toRegister, this.replace);

        // new implicit transaction
        t = t.fork();
        return resultNoData('CREATE', this.fn, t);
    }
}
