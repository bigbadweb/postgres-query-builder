import QueryBuilder from './queryBuilder';

const builder = QueryBuilder();

const query = builder.select('table_alias.column_name', 'col_alias')
                    .from('table_name', 'table_alias')
                    .whereEquals('col_alias.column_name', 'value')
                    .sql();

console.log('TypeScript example 1:');
console.log(query.sql);
console.log(query.params);

const builder2 = QueryBuilder();

const query2 = builder2
    .select(`tp.teacher_name`)
    .select(`tp.id`, 'teacher_profile_id')
    .from('course', 'c')
    .join('teacher_course', 'tc.course_id = c.id', 'tc')
    .join('teacher_profile', 'tp.user_id = tc.teacher_id', 'tp')
    .join('users', 'u.id = tp.user_id', 'u')
    .whereIsTrue('u.is_teacher')
    .whereEquals('c.status', 'my-status')
    .whereIsNotNull('tp.teacher_name')
    .groupBy('tp.teacher_name')
    .groupBy('tp.id')
    .orderBy('tp.teacher_name', 'ASC')
    .pagination({ page: 1, per: 10}, 'tp.id')
    .filter({ 'tp.teacher_name': ["John Doe"] })
    .sql();

console.log('\nTypeScript example 2:');
console.log(query2.sql);
console.log(query2.params);

// Test new null methods that were missing
const builder3 = QueryBuilder();

const query3 = builder3
    .select('*')
    .from('users', 'u')
    .whereIsNull('u.deleted_at')
    .whereNull('u.archived_at')
    .whereIsNotNull('u.email')
    .sql();

console.log('\nTypeScript example 3 (testing null methods):');
console.log(query3.sql);
console.log(query3.params);