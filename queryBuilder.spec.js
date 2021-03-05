const QueryBuilder = require('./queryBuilder');

const builder = new QueryBuilder();

const query = builder.select('table_alias.column_name', 'col_alias')
                    .from('table_name', 'table_alias')
                    .whereEquals('col_alias.column_name', 'value')
                    .sql();

console.log(query.sql);
console.log(query.params);
