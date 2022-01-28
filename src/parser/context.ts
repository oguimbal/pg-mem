import { _ISchema, _ISelection, _IDb, OnStatementExecuted, nil, QueryError, _IStatement } from '../interfaces-private';


class StackOf<T> {
    private stack: T[] = [];
    constructor(private name: string) {
    }
    usingValue = <ret>(value: T, act: () => ret): ret => {
        this.stack.push(value);
        try {
            return act();
        } finally {
            this.stack.pop();
        }
    }
    get current() {
        if (!this.stack.length) {
            throw new Error(`No ${this.name} available`);
        }
        return this.stack[this.stack.length - 1];
    }

    get currentOrNil() {
        return this.stack[this.stack.length - 1];
    }
}


const _selectionStack = new StackOf<_ISelection>('build context');
const _statementStack = new StackOf<_IStatement>('execution statement');
const _tempBindings = new StackOf<Map<string, _ISelection | 'no returning'>>('binding context');



interface IBuildContext {
    readonly selection: _ISelection;
    readonly db: _IDb;
    readonly schema: _ISchema;
    readonly onFinishExecution: (callback: OnStatementExecuted) => void
    readonly getTempBinding: (name: string) => _ISelection | nil;
    readonly setTempBinding: (name: string, boundTo: _ISelection) => void;
}

class Context implements IBuildContext {
    get selection(): _ISelection {
        return _selectionStack.current;
    }
    get db(): _IDb {
        return _selectionStack.current.db;
    }
    get schema(): _ISchema {
        // remove the concept of selection schema ?
        // (does not make much sens, if you think about it)
        return _selectionStack.current.ownerSchema;
    }
    onFinishExecution = (callback: OnStatementExecuted) => {
        _statementStack.current.onExecuted(callback);
    };
    getTempBinding = (name: string) => {
        const ret = _tempBindings.currentOrNil?.get(name);
        if (ret === 'no returning') {
            throw new QueryError(`WITH query "${name}" does not have a RETURNING clause`);
        }
        return ret;
    };
    setTempBinding = (name: string, boundTo: _ISelection) => {
        if (this.getTempBinding(name)) {
            throw new QueryError(`WITH query name "${name}" specified more than once`);
        }
        _tempBindings.current.set(name, boundTo.isExecutionWithNoResult ? 'no returning' : boundTo);
    };
}




const _buildCtx = new Context();
export function buildCtx(): IBuildContext {
    return _buildCtx;
}


export const withSelection = _selectionStack.usingValue;
export const withStatement = _statementStack.usingValue;
export function withBindingScope<T>(act: () => T): T {
    return _tempBindings.usingValue(new Map(), act);
}
