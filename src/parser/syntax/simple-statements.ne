
@lexer lexer
@include "base.ne"


simplestatements_all
    -> simplestatements_start_transaction
    | simplestatements_commit
    | simplestatements_rollback



# https://www.postgresql.org/docs/12/sql-start-transaction.html
simplestatements_start_transaction -> kw_start kw_transaction {% () => ({ type: 'start transaction' }) %}

# https://www.postgresql.org/docs/12/sql-commit.html
simplestatements_commit -> kw_commit {% () => ({ type: 'commit' }) %}

# https://www.postgresql.org/docs/12/sql-rollback.html
simplestatements_rollback -> kw_rollback {% () => ({ type: 'rollback' }) %}
