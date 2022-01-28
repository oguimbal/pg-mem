import { _ISchema, _ISelection, _IDb, OnStatementExecuted, nil, QueryError, _IStatement, IValue, Parameter } from '../interfaces-private';


class StackOf<T> {
    readonly stack: T[] = [];
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
    get current(): T {
        if (!this.stack.length) {
            throw new Error(`No ${this.name} available`);
        }
        return this.stack[this.stack.length - 1];
    }

    get currentOrNil(): T | nil {
        return this.stack[this.stack.length - 1];
    }
}


const _selectionStack = new StackOf<_ISelection>('build context');
const _statementStack = new StackOf<_IStatement>('execution statement');
const _tempBindings = new StackOf<Map<string, _ISelection | 'no returning'>>('binding context');
const _parametersStack = new StackOf<Parameter[]>('parameter context');
const _nameResolver = new StackOf<INameResolver>('name resolver');

export interface INameResolver {
    /** Try to resolve a name */
    resolve(name: string): IValue | nil;
    /** True if is isolated... meaning that one cannot fetch values from its parent */
    readonly isolated: boolean;
}


interface IBuildContext {
    readonly selection: _ISelection;
    readonly db: _IDb;
    readonly schema: _ISchema;
    readonly onFinishExecution: (callback: OnStatementExecuted) => void
    readonly getTempBinding: (name: string) => _ISelection | nil;
    readonly setTempBinding: (name: string, boundTo: _ISelection) => void;
    readonly getParameter: (nameOrPosition: string | number) => IValue | nil;
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
        if (_tempBindings.current.has(name)) {
            throw new QueryError(`WITH query name "${name}" specified more than once`);
        }
        _tempBindings.current.set(name, boundTo.isExecutionWithNoResult ? 'no returning' : boundTo);
    };
    getParameter = (nameOrPosition: string | number) => {
        const params = _parametersStack.currentOrNil;
        if (!params) {
            return null;
        }
        if (typeof nameOrPosition === 'number') {
            const ret = params[nameOrPosition]?.value;
            if (!ret) {
                // not ideal... (duplicated error message)
                throw new QueryError(`bind message supplies ${params.length} parameters, but prepared statement "" requires ${nameOrPosition}`, '08P01');
            }
            return ret;
        }
        return params.find(p => p.value.id === nameOrPosition)?.value;
    }
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
export const withParameters = _parametersStack.usingValue;
export const withNameResolver = _nameResolver.usingValue;

export function resolveName(name: string): IValue | null {
    for (let i = _nameResolver.stack.length - 1; i >= 0; i--) {
        const resolver = _nameResolver.stack[i];
        const found = resolver.resolve(name);
        if (found) {
            return found;
        }
        if (resolver.isolated) {
            return null;
        }
    }
    return null;
}
