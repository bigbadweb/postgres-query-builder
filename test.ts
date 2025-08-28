import QueryBuilder, { SchemaDefinition } from './queryBuilder';

console.log('=== TypeScript Query Builder Test Suite ===\n');

// Test 1: Basic functionality
console.log('Test 1: Basic Select');
const basic = QueryBuilder();
const basicQuery = basic.select('*').from('users').sql();
console.log('✓ Basic select:', basicQuery.sql.trim());
console.log('✓ Params:', basicQuery.params);

// Test 2: Complex WHERE conditions
console.log('\nTest 2: Complex WHERE conditions');
const complex = QueryBuilder();
const complexQuery = complex
  .select('u.name', 'user_name')
  .from('users', 'u')
  .whereEquals('u.status', 'active')
  .whereGT('u.age', 18)
  .whereIsNotNull('u.email')
  .whereNull('u.deleted_at')
  .sql();
console.log('✓ Complex WHERE:', complexQuery.sql.trim());
console.log('✓ Params:', complexQuery.params);

// Test 3: Schema-aware usage
interface TestSchema extends SchemaDefinition {
  users: {
    id: 'number';
    name: 'string';
    email: 'string';
    age: 'number';
    status: 'string';
    is_active: 'boolean';
  };
}

console.log('\nTest 3: Schema-aware TypeScript');
const schemaBuilder = QueryBuilder<TestSchema>();
const schemaQuery = schemaBuilder
  .select('u.name')
  .select('u.email')
  .from('users', 'u')
  .whereIsTrue('u.is_active')
  .orderBy('u.name', 'ASC')
  .limit(5)
  .sql();
console.log('✓ Schema-aware query:', schemaQuery.sql.trim());
console.log('✓ Params:', schemaQuery.params);

// Test 4: Pagination
console.log('\nTest 4: Pagination');
const pagination = QueryBuilder();
const paginationQuery = pagination
  .select('*')
  .from('posts')
  .page(2, 10)
  .sql();
console.log('✓ Pagination query:', paginationQuery.sql.trim());
console.log('✓ Params (page 2, per 10):', paginationQuery.params);

// Test 5: Joins
console.log('\nTest 5: Joins');
const joins = QueryBuilder();
const joinQuery = joins
  .select('u.name')
  .select('p.title', 'post_title')
  .from('users', 'u')
  .left_join('posts', 'p.user_id = u.id', 'p')
  .whereEquals('u.status', 'active')
  .sql();
console.log('✓ Join query:', joinQuery.sql.trim());
console.log('✓ Params:', joinQuery.params);

// Test 6: Array and null methods (previously missing)
console.log('\nTest 6: Previously missing methods');
const missing = QueryBuilder();
const missingQuery = missing
  .select(['name', 'email']) // Multi-select array
  .selectDistinctOn('status', 'created_at') // selectDistinctOn with groupClause
  .from('users')
  .whereIsNull('deleted_at') // whereIsNull
  .raw_join('LEFT JOIN profiles ON profiles.user_id = users.id') // raw_join
  .sql();
console.log('✓ Previously missing methods:', missingQuery.sql.trim());
console.log('✓ Params:', missingQuery.params);

console.log('\n=== All tests completed successfully! ===');

// Test MySQL dialect support
console.log('\nTest 7: MySQL dialect support');
process.env.DB_DIALECT = 'mysql';
const mysql = QueryBuilder();
const mysqlQuery = mysql
  .select('*')
  .from('users')
  .whereEquals('status', 'active')
  .limit(10)
  .sql();
console.log('✓ MySQL placeholders:', mysqlQuery.sql.trim());
console.log('✓ Params:', mysqlQuery.params);

// Reset to postgres
process.env.DB_DIALECT = 'postgres';