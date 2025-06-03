const QueryBuilder = require('./queryBuilder').QueryBuilder;

const builder = new QueryBuilder();

const query = builder.select('table_alias.column_name', 'col_alias')
                    .from('table_name', 'table_alias')
                    .whereEquals('col_alias.column_name', 'value')
                    .sql();


console.log(builder);
console.log(query.sql);
console.log(query.params);


const builder2 = new QueryBuilder();

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
    .sql();

console.log(query2.sql);
console.log(query2.params);
