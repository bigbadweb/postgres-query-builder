# Posgres Database Wrapper


## Install

`npm install --save https://github.com/bigbadweb/postgres-query-builder.git`

## Usage


```.js
const QueryBuilder = require('@bigbadweb/postgres-query-builder');

const builder = new QueryBuilder();

const query = builder.select('table_alias.column_name', 'col_alias')
                    .from('table_name', 'table_alias')
                    .whereEquals('col_alias.column_name', 'value')
                    .sql();
```


```
{
 sql: `
SELECT
            table_alias.column_name AS col_alias
        FROM
            table_name table_alias
        
        WHERE
            col_alias.column_name = $1`
 params:  [ 'value' ]
}
```

