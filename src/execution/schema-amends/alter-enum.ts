import {
    _ISchema,
    _Transaction,
    _IStatementExecutor,
    _IStatement,
} from '../../interfaces-private';
import {AlterEnumType, AlterSequenceStatement} from '@secam/pgsql-ast-parser';
import { ExecHelper } from '../exec-utils';
import { ignore } from '../../utils';
import {asEnum, CustomEnumType} from "../../datatypes/t-custom-enum";

export class AlterEnum extends ExecHelper implements _IStatementExecutor {
    private onSchema: _ISchema;
    private originalEnum: CustomEnumType;
    constructor({ schema }: _IStatement, private p: AlterEnumType) {
        super(p);
        this.onSchema = schema.getThisOrSiblingFor(p.name);
        this.originalEnum = asEnum(schema.getObject(p.name))
        if (!this.onSchema) {
            ignore(this.p)
        }
    }

    execute(t: _Transaction) {
        // commit pending data before making changes
        //  (because the index sequence creation does support further rollbacks)
        t = t.fullCommit();
        const enumValues = this.originalEnum.values

        switch (this.p.change.type) {
            case 'add value':
                enumValues.push(this.p.change.add.value)
                break;
            case 'rename':
                this.originalEnum.drop(t)
                this.onSchema.registerEnum(this.p.change.to.name, enumValues)
                break;
        }

        // new implicit transaction
        t = t.fork();

        return this.noData(t, 'ALTER');
    }
}
