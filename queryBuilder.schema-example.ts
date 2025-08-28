import QueryBuilder, { SchemaDefinition } from './queryBuilder';

// Define your database schema for better type safety
interface MySchema extends SchemaDefinition {
  users: {
    id: 'number';
    email: 'string';
    name: 'string';
    is_teacher: 'boolean';
    deleted_at: 'date';
    archived_at: 'date';
  };
  courses: {
    id: 'number';
    title: 'string';
    status: 'string';
  };
  teacher_profile: {
    id: 'number';
    user_id: 'number';
    teacher_name: 'string';
  };
  teacher_course: {
    id: 'number';
    teacher_id: 'number';
    course_id: 'number';
  };
}

console.log('=== Schema-aware TypeScript Query Builder Examples ===\n');

// Create a schema-aware query builder
const builder = QueryBuilder<MySchema>();

const query1 = builder
    .select('u.email')
    .select('u.name', 'user_name')
    .from('users', 'u')
    .whereEquals('u.is_teacher', true)
    .whereIsNotNull('u.email')
    .limit(10)
    .sql();

console.log('Schema-aware example 1:');
console.log(query1.sql);
console.log(query1.params);

// More complex example with joins
const builder2 = QueryBuilder<MySchema>();

const query2 = builder2
    .select('tp.teacher_name')
    .select('c.title', 'course_title')
    .select('u.email', 'teacher_email')
    .from('courses', 'c')
    .join('teacher_course', 'tc.course_id = c.id', 'tc')
    .join('teacher_profile', 'tp.user_id = tc.teacher_id', 'tp')
    .join('users', 'u.id = tp.user_id', 'u')
    .whereEquals('c.status', 'active')
    .whereIsTrue('u.is_teacher')
    .whereIsNotNull('tp.teacher_name')
    .orderBy('tp.teacher_name', 'ASC')
    .sql();

console.log('\nSchema-aware example 2:');
console.log(query2.sql);
console.log(query2.params);

// Test schema-less (backward compatibility)
const legacyBuilder = QueryBuilder();

const query3 = legacyBuilder
    .select('*')
    .from('any_table')
    .whereEquals('any_column', 'any_value')
    .sql();

console.log('\nBackward compatibility example:');
console.log(query3.sql);
console.log(query3.params);