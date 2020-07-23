export type Statement = SelectStatement
    | CreateTableStatement
    | CreateIndexStatement
    | CommitStatement
    | InsertStatement
    | UpdateStatement
    | RollbackStatement
    | StartTransactionStatement;


export interface StartTransactionStatement {
    type: 'start transaction';
}
export interface CommitStatement {
    type: 'commit';
}
export interface RollbackStatement {
    type: 'rollback';
}


export interface InsertStatement {
    type: 'insert';
    into: TableRefAliased;
    returning?: SelectedColumn[];
    columns?: string[];
    /** Insert values */
    values?: Expr[][];
    /** Insert into select */
    select?: SelectStatement;
}

export interface CreateIndexStatement {
    type: 'create index';
    table: string;
    expressions: IndexExpression[];
    unique?: true;
    ifNotExists?: true;
    indexName?: string;
}

export interface IndexExpression {
    expression: Expr;
    order?: 'asc' | 'desc';
    nulls?: 'first' | 'last';
}

export interface CreateTableStatement {
    type: 'create table';
    name: string;
    ifNotExists?: true;
    columns: CreateColumnDef[];
    /** Constraints not defined inline */
    constraints?: TableConstraint[];
}

export interface CreateColumnDef {
    name: string;
    dataType: DataTypeDef;
    // collate?: string; (todo)
    constraint?: ColumnConstraint;
}

export interface DataTypeDef {
    type: string;
    length?: number;
    arrayOf?: DataTypeDef;
}

export type ColumnConstraint = UniqueConstraint | PrimaryConstraint | NotNullConstraint;

export interface NotNullConstraint {
    type: 'not null';
}

export interface PrimaryConstraint {
    type: 'primary key';
}

export interface UniqueConstraint {
    type: 'unique';
    notNull?: boolean;
}

export type TableConstraint = (PrimaryConstraint | UniqueConstraint) & {
    constraintName?: string;
    columns: string[];
}


export interface SelectStatement {
    type: 'select',
    columns?: SelectedColumn[];
    from?: From[];
    where?: Expr;
}


export interface UpdateStatement {
    type: 'update';
    table: TableRefAliased;
    sets: SetStatement[];
    where?: Expr;
    returning?: SelectedColumn[];
}

export interface SetStatement {
    column: string;
    value: Expr | 'default';
}

export interface SelectedColumn {
    expr: Expr;
    alias?: string;
}

export type From = FromTable | FromStatement;

export interface TableRef {
    table: string;
    db?: string;
}

export interface TableRefAliased extends TableRef {
    alias?: string;
}

export interface FromTable extends TableRefAliased {
    type: 'table',
    join?: JoinClause;
}

export interface FromStatement {
    type: 'statement';
    statement: SelectStatement;
    alias: string;
    db?: null;
    join?: JoinClause;
}

export interface JoinClause {
    type: JoinType;
    on?: Expr;
}

export type JoinType = 'INNER JOIN'
    | 'LEFT JOIN'
    | 'RIGHT JOIN'
    | 'FULL JOIN';

export type Expr = ExprRef
    | ExprList
    | ExprNull
    | ExprInteger
    | ExprMember
    | ExprArrayIndex
    | ExprNumeric
    | ExprString
    | ExprCase
    | ExprBinary
    | ExprUnary
    | ExprCast
    | ExprBool
    | ExprCall
    | SelectStatement
    | ExprTernary;


export type LogicOperator = 'OR' | 'AND';
export type EqualityOperator = 'IN' | 'NOT IN' | 'LIKE' | 'NOT LIKE' | 'ILIKE' | 'NOT ILIKE' | '=' | '!=';
export type ComparisonOperator = '>' | '>=' | '<' | '<=' | '@>' | '<@' | '?' | '?|' | '?&';
export type AdditiveOperator = '||' | '-' | '#-' | '&&' | '+';
export type MultiplicativeOperator = '*' | '%' | '/';
export type BinaryOperator = LogicOperator
    | EqualityOperator
    | ComparisonOperator
    | AdditiveOperator
    | MultiplicativeOperator
    | '^'

export interface ExprBinary {
    type: 'binary';
    left: Expr;
    right: Expr;
    op: BinaryOperator;
}


export interface ExprTernary {
    type: 'ternary';
    value: Expr;
    lo: Expr;
    hi: Expr;
    op: 'BETWEEN' | 'NOT BETWEEN';
}

export interface ExprCast {
    type: 'cast';
    to: DataTypeDef;
    operand: Expr;
}


export type UnaryOperator = '+' | '-' | 'NOT' | 'IS NULL' | 'IS NOT NULL' | 'IS TRUE' | 'IS FALSE' | 'IS NOT TRUE' | 'IS NOT FALSE';
export interface ExprUnary {
    type: 'unary';
    operand: Expr;
    op: UnaryOperator;
}

export interface ExprRef {
    type: 'ref';
    table?: string;
    name: string | '*';
}

export interface ExprMember {
    type: 'member';
    operand: Expr;
    op: '->' | '->>';
    member: string | number;
}

export interface ExprCall {
    type: 'call';
    function: string;
    args: Expr[];
}

export interface ExprList {
    type: 'list';
    expressions: Expr[];
}

export interface ExprArrayIndex {
    type: 'arrayIndex',
    array: Expr;
    index: Expr;
}

export interface ExprNull {
    type: 'null';
}

export interface ExprInteger {
    type: 'integer';
    value: number;
}

export interface ExprNumeric {
    type: 'numeric';
    value: number;
}

export interface ExprString {
    type: 'string';
    value: string;
}

export interface ExprBool {
    type: 'boolean';
    value: boolean;
}

export interface ExprCase {
    type: 'case';
    value?: Expr;
    whens: ExprWhen[];
    else?: Expr;
}

export interface ExprWhen {
    when: Expr;
    value: Expr;
}