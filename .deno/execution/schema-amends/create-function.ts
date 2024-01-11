import { _Transaction, _ISchema, NotSupported, _ITable, _IStatement, _IStatementExecutor, QueryError, _ArgDefDetails, IType, _IType, nil, FunctionDefinition } from '../../interfaces-private.ts';
import { CreateFunctionStatement } from 'https://deno.land/x/pgsql_ast_parser@12.0.1/mod.ts';
import { ExecHelper } from '../exec-utils.ts';
import { buildValue } from '../../parser/expression-builder.ts';
import { Types } from '../../datatypes/index.ts';
import { ignore, deepEqual } from '../../utils.ts';
import { withSelection } from '../../parser/context.ts';

export class CreateFunction extends ExecHelper implements _IStatementExecutor {
    private onSchema: _ISchema;
    private toRegister: FunctionDefinition;
    private replace: boolean;

    constructor({ schema }: _IStatement, fn: CreateFunctionStatement) {
        super(fn);
        if (!fn.language) {
            throw new QueryError('Unspecified function language');
        }
        this.onSchema = schema.getThisOrSiblingFor(fn.name);

        const lang = schema.db.getLanguage(fn.language.name);

        // determine arg types
        const args = withSelection(schema.dualTable.selection, () => fn.arguments.map<_ArgDefDetails>(a => ({
            name: a.name?.name,
            type: schema.getType(a.type),
            default: a.default && buildValue(a.default),
            mode: a.mode,
        })));

        // determine return type
        let returns: IType | null = null;
        if (!fn.returns) {
            throw new QueryError('Unspecified function return type');
        }
        if (typeof fn.code !== 'string') {
            throw new QueryError('no function body specified');
        }
        switch (fn.returns.kind) {
            case 'table':
                const columns = fn.returns.columns.map(c => ({
                    name: c.name.name,
                    type: schema.getType(c.type),
                }));
                returns = Types.record(columns).asArray();
                break;
            case 'array':
            case null:
            case undefined:
                returns = schema.getType(fn.returns);
                break;
            default:
                throw NotSupported.never(fn.returns);
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
            functionName: fn.name.name,
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

        // if the function exists
        const existing = this.onSchema.getFunction(this.toRegister.name, args.map(x => x.type));
        if (existing) {
            if (!this.replace) {
                throw new QueryError(`function ${this.toRegister.name} lready exists with same argument types`, '42723');
            }

            //  ... it must be the same type
            if (existing.returns !== returns) {
                throw new QueryError(`cannot change return type of existing function`, '42P13');
            }

            // ... argument names must be the same
            for (let i = 0; i < args.length; i++) {
                const exName = existing.args[i].name
                if (exName ?? null !== args[i].name ?? null) {
                    throw new QueryError(`cannot change name of input parameter "${exName}"`, '42P13');
                }
            }
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because does not support further rollbacks)
        t = t.fullCommit();

        this.onSchema.registerFunction(this.toRegister, this.replace);

        // new implicit transaction
        t = t.fork();
        return this.noData(t, 'CREATE');
    }
}
