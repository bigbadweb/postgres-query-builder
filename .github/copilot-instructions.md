# PostgreSQL Query Builder Library

PostgreSQL Query Builder is a Node.js library that provides a fluent API for building SQL queries for PostgreSQL (and optionally MySQL) databases. The library generates parameterized SQL queries with proper escaping.

**CRITICAL**: Always reference these instructions first and fallback to search or bash commands only when you encounter unexpected information that does not match the info here.

## Working Effectively

### Bootstrap and Setup
- **Install dependencies**: `npm install` -- takes 3-5 seconds. NEVER CANCEL. Set timeout to 30+ seconds.
  - Note: You will see many deprecation warnings - this is expected and does not affect functionality
  - The package-lock.json will be updated automatically
- **Verify installation**: `node queryBuilder.examples.js` -- takes <1 second. This validates the core functionality works.

### No Build Required
- This is pure JavaScript with no build step required
- The main entry point is `queryBuilder.js` 
- No compilation, transpilation, or bundling needed

### Testing and Validation
- **CRITICAL**: There is no formal test suite in this repository
- The `npm test` command is not configured and will fail with "Error: no test specified"
- Jest is available in devDependencies (v26.6.3) but no test files exist
- **ALWAYS validate changes manually** using these approaches:
  1. Run the examples: `node queryBuilder.examples.js`
  2. Create manual test scripts in `/tmp` directory
  3. Test both PostgreSQL and MySQL dialects: `DB_DIALECT=mysql node your-test.js`

### Manual Validation Scenarios
After making any changes, ALWAYS run these validation steps:
1. **Basic functionality**: `node -e "const QB = require('./queryBuilder'); const qb = new QB.QueryBuilder(); console.log(qb.select('*').from('users').sql());"`
2. **Complex query**: Run `node queryBuilder.examples.js` and verify both queries generate correct SQL
3. **Parameter binding**: Ensure queries with parameters generate correct placeholder syntax ($1, $2, etc. for PostgreSQL)
4. **MySQL compatibility**: Test with `DB_DIALECT=mysql node your-test.js` to ensure MySQL mode works (`?` placeholders)
   - **IMPORTANT**: The DB_DIALECT environment variable must be set BEFORE the module is loaded
5. **Create manual test script**: Copy the validation script pattern from `/tmp/manual-test.js` example

## Repository Structure

### Key Files
- `queryBuilder.js` (21KB) - Main library implementation containing the QueryBuilder class
- `queryBuilder.examples.js` - Working examples demonstrating library usage  
- `package.json` - Package configuration (Jest in devDependencies, no other dependencies)
- `README.md` - Basic usage documentation

### Common Patterns
```bash
# Repository root structure
ls -la /home/runner/work/postgres-query-builder/postgres-query-builder
total 232
drwxr-xr-x 3 runner docker   4096 Aug 28 08:20 .
drwxr-xr-x 3 runner docker   4096 Aug 28 08:19 ..
drwxr-xr-x 7 runner docker   4096 Aug 28 08:20 .git
-rw-r--r-- 1 runner docker     13 Aug 28 08:20 .gitignore
-rw-r--r-- 1 runner docker    695 Aug 28 08:20 README.md
-rw-r--r-- 1 runner docker 183716 Aug 28 08:20 package-lock.json
-rw-r--r-- 1 runner docker    745 Aug 28 08:20 package.json
-rw-r--r-- 1 runner docker   1065 Aug 28 08:20 queryBuilder.examples.js
-rw-r--r-- 1 runner docker  21022 Aug 28 08:20 queryBuilder.js
```

## Usage Patterns

### Basic Import and Usage
```javascript
const QueryBuilder = require('./queryBuilder').QueryBuilder;
const builder = new QueryBuilder();
const query = builder.select('*').from('users').whereEquals('id', 123).sql();
// Returns: { sql: "SELECT...", params: [123] }
```

### Factory Function Alternative
```javascript
const queryBuilder = require('./queryBuilder');  // Returns factory function
const builder = queryBuilder();  // Creates new instance
```

## Development Guidelines

### Making Changes
1. **ALWAYS** test changes with `node queryBuilder.examples.js` first
2. **NEVER** skip manual validation - there are no automated tests
3. Test both database dialects:
   - PostgreSQL (default): parameters use $1, $2, $3, etc.
   - MySQL: `DB_DIALECT=mysql node your-test.js` - parameters use ?
4. Create validation scripts in `/tmp` to avoid committing test files

### Key Implementation Details
- The QueryBuilder class constructor initializes empty arrays for columns, tables, wheres, joins, etc.
- SQL generation happens in `_generateSQL()` method
- Parameter binding uses `_addParam()` method which handles dialect differences
- Fluent API pattern - most methods return `this` for chaining

### No CI/CD Pipeline
- There are no GitHub Actions workflows
- No automated testing or deployment
- No linting configuration (no ESLint, Prettier, etc.)
- Manual validation is your only quality gate

## Troubleshooting

### Common Issues
- `npm test` fails - Expected. No test suite is configured.
- Deprecation warnings during `npm install` - Expected. Does not affect functionality.
- Missing test files - Expected. This library relies on manual testing.

### Performance Notes
- `npm install`: ~3-5 seconds (with warnings)
- Running examples: <1 second
- Library loading: Nearly instantaneous
- No memory-intensive operations

## Environment Variables
- `DB_DIALECT`: Set to 'mysql' for MySQL parameter syntax (?), defaults to 'postgres' ($1, $2, etc.)
  - **CRITICAL**: Must be set BEFORE loading the module: `DB_DIALECT=mysql node your-script.js`
  - Cannot be changed after module is loaded

## Manual Testing Example
Since there's no formal test suite, create manual validation scripts like this:

```javascript
// Save as /tmp/manual-test.js
const QueryBuilder = require('./queryBuilder').QueryBuilder;

console.log('🧪 Testing QueryBuilder...');

// Test basic functionality
const qb = new QueryBuilder();
const result = qb.select('*').from('users').whereEquals('active', true).sql();
console.log('✓ SQL generated:', result.sql.replace(/\n/g, ' '));
console.log('✓ Parameters:', result.params);

// Test MySQL dialect - run with: DB_DIALECT=mysql node /tmp/manual-test.js
if (process.env.DB_DIALECT === 'mysql') {
    console.log('✓ MySQL dialect active - using ? placeholders');
}

console.log('✅ Manual test completed');
```

## Comprehensive End-to-End Validation
For thorough validation after major changes, create and run a comprehensive test file:

```bash
# Create comprehensive test file (example pattern) - saves in current directory  
echo '// Comprehensive QueryBuilder validation
const QB = require("./queryBuilder").QueryBuilder;
console.log("🔍 Running comprehensive validation...");
let tests = 0, passed = 0;
function test(name, fn) { tests++; try { if(fn()) { console.log(`✅ ${name}`); passed++; } else console.log(`❌ ${name}`); } catch(e) { console.log(`❌ ${name}: ${e.message}`); }}
test("Library loads", () => typeof QB === "function");
test("Creates instance", () => { const q = new QB(); return q && typeof q.select === "function"; });
test("Builds SELECT", () => { const r = new QB().select("*").from("users").sql(); return r.sql && Array.isArray(r.params); });
test("WHERE binding", () => { const r = new QB().select("*").from("users").whereEquals("id", 1).sql(); return r.params[0] === 1; });
test("JOIN support", () => { const r = new QB().select("u.name").from("users","u").join("posts","p.user_id=u.id","p").sql(); return r.sql.includes("JOIN"); });
console.log(`📊 ${passed}/${tests} tests passed`); process.exit(passed === tests ? 0 : 1);' > comprehensive-test.js

# Run the comprehensive test
node comprehensive-test.js

# Clean up
rm comprehensive-test.js
```

## Validation Commands Reference

Quick validation commands to run after changes:
```bash
# Basic setup validation
npm install && node queryBuilder.examples.js

# Quick functionality test  
node -e "const QB=require('./queryBuilder'); console.log('✓ Library loads'); const q=new QB.QueryBuilder().select('*').from('test').sql(); console.log('✓ Query builds:', q.sql)"

# Dialect testing
DB_DIALECT=mysql node -e "const QB=require('./queryBuilder'); const q=new QB.QueryBuilder().select('*').from('test').whereEquals('id',1).sql(); console.log('MySQL syntax:', q)"

# Parameter binding test  
node -e "const QB=require('./queryBuilder'); const q=new QB.QueryBuilder().select('*').from('users').whereEquals('name','test').whereGT('age',25).sql(); console.log('Parameters:', q.params, 'SQL:', q.sql)"

# Complex query test
node -e 'const QB=require("./queryBuilder"); const q=new QB.QueryBuilder().select("u.name").from("users","u").join("posts","p.user_id=u.id","p").whereIsTrue("u.active").sql(); console.log("Complex query works:", !!q.sql);'
```

**Remember**: This is a library, not an application. There is no server to run, no UI to test, and no database connections. Focus on SQL query generation correctness and API functionality.