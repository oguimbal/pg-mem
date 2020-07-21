@{% const {lexer} = require('../lexer.ts'); %}
@lexer lexer
@include "base.ne"
@include "expr.ne"
@include "select.ne"
@include "create-table.ne"


# list of statements, separated by ";"
main -> statement {% unwrap %} # _ statement:? (_ %semicolon _ statement:?):*


statement
    -> select_statement
    | createtable_statement
