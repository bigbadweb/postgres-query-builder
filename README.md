# PostgreSQL Database Query Builder

A simple fluent query builder for PostgreSQL with full TypeScript support and optional schema typing.

## Features

- 🔧 Fluent API for building complex SQL queries
- 🚀 Full TypeScript support with type safety
- 📋 Optional schema definition (Drizzle-like approach)
- 🔄 Backward compatible with JavaScript
- 🔍 Comprehensive WHERE clause methods
- 📊 Built-in pagination support
- 🔗 Support for JOIN operations
- 🎯 Filtering and searching capabilities

## Install

```bash
npm install --save https://github.com/bigbadweb/postgres-query-builder.git
```

## Usage

### JavaScript (Legacy)

```js
const QueryBuilder = require('@bigbadweb/postgres-query-builder');

const builder = QueryBuilder();

const query = builder.select('table_alias.column_name', 'col_alias')
                    .from('table_name', 'table_alias')
                    .whereEquals('col_alias.column_name', 'value')
                    .sql();
```

### TypeScript (Recommended)

```typescript
import QueryBuilder from '@bigbadweb/postgres-query-builder';

const builder = QueryBuilder();

const query = builder.select('table_alias.column_name', 'col_alias')
                    .from('table_name', 'table_alias')
                    .whereEquals('col_alias.column_name', 'value')
                    .sql();
```

### Schema-aware TypeScript

```typescript
import QueryBuilder, { SchemaDefinition } from '@bigbadweb/postgres-query-builder';

// Define your database schema for better type safety
interface MySchema extends SchemaDefinition {
  users: {
    id: 'number';
    email: 'string';
    name: 'string';
    is_teacher: 'boolean';
  };
  courses: {
    id: 'number';
    title: 'string';
    status: 'string';
  };
}

const builder = QueryBuilder<MySchema>();

const query = builder
  .select('u.email')
  .select('u.name', 'user_name')
  .from('users', 'u')
  .whereEquals('u.is_teacher', true)
  .whereIsNotNull('u.email')
  .sql();
```

### Query Result

```js
const query = {
  sql: `
SELECT
  table_alias.column_name AS col_alias
FROM
  table_name table_alias
WHERE
  col_alias.column_name = $1`,
  params: ['value']
}
```

## API Reference

### Selection Methods
- `select(column, alias?)` - Select columns with optional alias
- `selectDistinct(column, alias?)` - Select distinct values
- `selectDistinctOn(column, groupClause?, alias?)` - Select distinct on specific column

### Table Methods
- `from(table, alias?)` - Set the main table

### Join Methods
- `join(table, onClause, alias?)` - Inner join
- `left_join(table, onClause, alias?)` - Left join
- `right_join(table, onClause, alias?)` - Right join
- `raw_join(joinClause)` - Raw join clause

### WHERE Clause Methods
- `where(clause, joinType?)` - Raw WHERE clause
- `whereEquals(column, value, joinType?)` - Equality condition
- `whereNotEquals(column, value, joinType?)` - Not equals condition
- `whereIsTrue(column, joinType?)` - Boolean true condition
- `whereIsFalse(column, joinType?)` - Boolean false condition
- `whereLike(column, pattern, joinType?, ignoreCase?)` - LIKE condition
- `whereNull(column, joinType?, operator?)` - NULL condition
- `whereIsNull(column, joinType?)` - IS NULL condition
- `whereIsNotNull(column, joinType?)` - IS NOT NULL condition
- `whereGT(column, value, joinType?)` - Greater than
- `whereGTE(column, value, joinType?)` - Greater than or equal
- `whereLT(column, value, joinType?)` - Less than
- `whereLTE(column, value, joinType?)` - Less than or equal
- `whereBetween(column, min, max, joinType?)` - BETWEEN condition
- `whereNotBetween(column, min, max, joinType?)` - NOT BETWEEN condition
- `whereIncludes(column, values, joinType?)` - IN condition

### Grouping and Ordering
- `groupBy(column)` - GROUP BY clause
- `orderBy(column, direction?, alias?)` - ORDER BY clause
- `sort(column, direction?, alias?)` - Alias for orderBy

### Pagination and Limiting
- `limit(limit)` - LIMIT clause
- `offset(offset)` - OFFSET clause
- `page(page, per)` - Pagination helper
- `pagination(config, countCol?, sortColAlias?)` - Advanced pagination

### Advanced Features
- `filter(filter, filterColAlias?)` - Complex filtering
- `search(search, searchCols, ignoreCase?)` - Text search
- `filterArray(filters, alias?, matchNull?, matchEmpty?)` - Array column filtering

### Query Generation
- `sql(formatted?)` - Generate SQL and parameters
- `dump()` - Console log the query

### Database Integration
- `findOne(db)` - Execute and return single result
- `findMany(db, customResultsFunc?)` - Execute and return multiple results

## TypeScript Types

The library exports the following TypeScript types:

- `QueryBuilder<TSchema>` - The main query builder class
- `SchemaDefinition` - Interface for defining database schemas
- `TableSchema` - Interface for defining table column types
- `ColumnType` - Union type for supported column types

## Development

```bash
# Install dependencies
npm install

# Build TypeScript
npm run build

# Run examples
node dist/queryBuilder.examples.js
```

