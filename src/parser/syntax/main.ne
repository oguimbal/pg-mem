@{% const {lexer} = require('../lexer.ts'); %}
@lexer lexer
@include "base.ne"
@include "expr.ne"
@include "select.ne"
@include "create-table.ne"
@include "create-index.ne"
@include "simple-statements.ne"
@include "insert.ne"


# list of statements, separated by ";"
main -> _ statement (statement_separator:+ _ statement {% last %}):* statement_separator:* _  {% ([_, head, _tail]) => {
    const tail = unwrap(_tail);
    if (tail) {
        return tail.length
            ? [unwrap(head), ...tail.map(unwrap)]
            : [unwrap(head), tail];
    }
    return unwrap(head);
} %}

statement_separator -> _ %semicolon


statement
    -> select_statement
    | createtable_statement
    | createindex_statement
    | simplestatements_all
    | insert_statement
