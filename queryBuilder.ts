const PAGINATION_COLS = [
    '_pagination_page',
    '_pagination_per_page',
    '_pagination_num_pages',
    '_pagination_total_items',
];

const getDialect = () => process.env.DB_DIALECT || 'postgres'; // default to postgres

// Basic column type definitions for type safety
type ColumnType = 'string' | 'number' | 'boolean' | 'date' | 'array' | 'json';

// Schema definition interface
interface TableSchema {
    [columnName: string]: ColumnType;
}

interface SchemaDefinition {
    [tableName: string]: TableSchema;
}

type PaginationAndSort = {
    page?: number;
    per?: number;
    sortBy?: string;
    sortDir?: string;
    includeMetadata?: boolean;
};

type Filter = {
    [key: string]: (string | number | null)[];
};

type Search = {
    query?: string;
};

type QueryBuilderConfig = {
    filter?: Filter | null;
    search?: Search | null;
    pagination?: PaginationAndSort | null;
};

type DB = {
    findOne: (sql: string, params: any[]) => Promise<any>;
    findMany: (sql: string, params: any[]) => Promise<any[]>;
};

// Generic QueryBuilder for type safety
class QueryBuilder<TSchema extends SchemaDefinition = SchemaDefinition> {
    private paginated: boolean = false;
    private includeMetadata: boolean = false;
    private columns: string[] = [];
    private tables: string[] = [];
    private wheres: string[] = [];
    private groups: string[] = [];
    private joins: string[] = [];
    private _limit?: string;
    private _offset?: string;
    private sorts: string[] = [];
    private params: any[] = [];
    public config: QueryBuilderConfig = { filter: null };

    select(column: string | string[], alias?: string): this {
        if (Array.isArray(column)) {
            return this._selectMulti(column);
        } else {
            return this._select(column, alias);
        }
    }

    private _select(column: string, alias?: string): this {
        this.columns.push(alias ? `${column} AS ${alias}` : column);
        return this;
    }

    private _selectMulti(columns: string[]): this {
        for (let col of columns) {
            this.select(col);
        }
        return this;
    }

    selectDistinct(column: string, alias?: string): this {
        return this.select(`DISTINCT ${column}`, alias);
    }

    selectDistinctOn(column: string, groupClause?: string, alias?: string): this {
        if (groupClause) {
            return this.select(`DISTINCT ON (${column}) ${groupClause}`, alias);
        }
        return this.select(`DISTINCT ON (${column})`, alias);
    }

    from(table: string, alias?: string): this {
        this.tables.push(alias ? `${table} ${alias}` : table);
        return this;
    }

    where(where: string, jointype?: string): this {
        if (this.wheres.length > 0) {
            jointype = jointype || 'AND';
        } else {
            // no jointype if no other wheres
            jointype = '';
        }
        this.wheres.push(`${jointype ? jointype : ''} ${where}`);
        return this;
    }

    whereEquals(column: string, param: any, jointype?: string): this {
        return this._where(column, param, jointype, '=');
    }
    whereNotEquals(column: string, param: any, jointype?: string): this {
        return this._where(column, param, jointype, '!=');
    }

    whereIsTrue(column: string, jointype?: string): this {
        return this._where(column, true, jointype, '=');
    }
    whereIsFalse(column: string, jointype?: string): this {
        return this._where(column, false, jointype, '=');
    }

    whereLike(column: string, param: string, jointype?: string, ignoreCase: boolean = true): this {
        if (ignoreCase) {
            return this.where(
                `LOWER(${column}) LIKE LOWER(${this._addParam(`%${param}%`)})`,
                jointype
            );
        } else {
            return this.where(
                `${column} LIKE ${this._addParam(`%${param}%`)}`,
                jointype
            );
        }
    }

    whereIsNot(column: string, param: any, jointype?: string): this {
        return this._where(column, param, jointype, ' IS NOT ');
    }

    whereNull(column: string, jointype?: string, operator: string = ' IS '): this {
        if (this.wheres.length > 0) {
            jointype = jointype || 'AND';
        }
        const clause = `${column} ${operator} NULL`;
        this.wheres.push(jointype ? `${jointype} ${clause}` : clause);
        return this;
    }

    whereIsNotNull(column: string, jointype?: string): this {
        return this.whereNull(column, jointype, ' IS NOT ');
    }

    whereIsNull(column: string, jointype?: string): this {
        return this._where(column, null, jointype, ' IS ');
    }

    whereGT(column: string, param: any, jointype?: string): this {
        return this._where(column, param, jointype, '>');
    }
    whereGTE(column: string, param: any, jointype?: string): this {
        return this._where(column, param, jointype, '>=');
    }
    whereLT(column: string, param: any, jointype?: string): this {
        return this._where(column, param, jointype, '<');
    }
    whereLTE(column: string, param: any, jointype?: string): this {
        return this._where(column, param, jointype, '<=');
    }

    whereBetween(column: string, min: any, max: any, jointype?: string): this {
        // WHERE column BETWEEN min AND max
        return this._between(column, min, max, false, jointype);
    }
    whereNotBetween(column: string, min: any, max: any, jointype?: string): this {
        // WHERE column NOT BETWEEN min AND max
        return this._between(column, min, max, true, jointype);
    }

    whereIncludes(column: string, valueList: any[], jointype?: string): this {
        return this.where(`${column} IN (${this._addParams(valueList)})`, jointype);
    }

    groupBy(column: string): this {
        this.groups.push(column);
        return this;
    }

    left_join(table: string, onClause: string, alias?: string): this {
        return this._join(table, onClause, alias, 'LEFT');
    }
    right_join(table: string, onClause: string, alias?: string): this {
        return this._join(table, onClause, alias, 'RIGHT');
    }

    join(table: string, onClause: string, alias?: string): this {
        return this._join(table, onClause, alias);
    }

    raw_join(join: string): this {
        this.joins.push(join);
        return this;
    }

    page(page: number, per: number): this {
        const limit = per;
        const offset = (page - 1) * per;
        
        this._limit = this._addParam(limit);
        this._offset = this._addParam(offset);
        return this;
    }

    limit(limit: number): this {
        this._limit = this._addParam(limit);
        return this;
    }

    offset(offset: number): this {
        this._offset = this._addParam(offset);
        return this;
    }

    /**
     * Takes a pagination object as per the pagination middleware
     * @param  {PaginationAndSort} pagination
     * @countCol {string} the column to use as an overall count
     * @return {QueryBuilder}
     */
    pagination(
        pagination?: PaginationAndSort,
        countCol?: string,
        sortColAlias?: string
    ): this {
        this.config.pagination = pagination;
        // don't do it twice
        if (this.paginated) {
            return this;
        }

        if (!pagination) {
            return this;
        }

        if (pagination.page && pagination.per) {
            this.page(pagination.page, pagination.per);
        }

        if (pagination.sortBy) {
            this.sort(pagination.sortBy, pagination.sortDir || undefined, sortColAlias);
        }

        if (pagination.includeMetadata && countCol) {
            this.paginationCounts(countCol, pagination);
        }

        this.paginated = true;
        return this;
    }

    paginationCounts(countCol: string, pagination: PaginationAndSort): this {
        // avoid doing it twice
        if (this.paginated) {
            return this;
        }
        // TODO: do we need to add information about all rows?
        this.select(`${this._addParam(pagination.page)}::int`, '_pagination_page');
        this.select(
            `${this._addParam(pagination.per)}::int`,
            '_pagination_per_page'
        );
        this.select(
            `CEIL((COUNT(${countCol}) OVER())::float  / ${this._addParam(
                pagination.per
            )})`,
            '_pagination_num_pages'
        ); // get's total pages
        this.select(`COUNT(${countCol}) OVER()::int`, '_pagination_total_items'); // get's total pages

        this.includeMetadata = true;
        return this;
    }

    search(search?: Search, searchCols: string[] = [], ignoreCase: boolean = true): this {
        this.config.search = search;

        // support single value
        if (!search || !search.query) {
            // WHERE LOWER(x) LIKE LOWER("%y%")
            return this;
        }

        if (!searchCols) {
            throw new Error('Missing search column'); // error?
        }

        if (searchCols && !Array.isArray(searchCols)) {
            searchCols = [searchCols];
        }

        if (searchCols.length == 0) {
            throw new Error('Missing search column'); // error?
        }

        /// Generate something like  AND (col LIKE (y) OR col2 LIKE (y))
        const paramName = this._addParam(`%${search.query}%`);
        const likes = searchCols.map((searchCol) => {
            if (ignoreCase) {
                return `LOWER(${searchCol}) LIKE LOWER(${paramName})`;
            }
            return `${searchCol} LIKE ${paramName}`;
        });

        // let jointype = 'AND';
        this.where(`(${likes.join(' OR ')})`, 'AND');
        return this;
    }

    /**
     * Generate filter SQL for the given object
     * @param  {Filter} filter         the filter configuration
     * @param  {string} filterColAlias the alias of the table that filters should be applied when there is ambiguity
     * @return {[type]}                [description]
     */
    filter(filter: Filter, filterColAlias?: string): this {
        this.config.filter = this.config.filter || {};
        Object.assign(this.config.filter, filter);
        let filterGroup: string[] = [];

        // This generates a structure with
        // IN () the same props
        // AND different props
        // like
        // WHERE ...
        //    -- filter group
        // 		AND
        // 			(
        // 				(	prop IN ($1, $2))
        // 					AND
        // 				( prop2  IN ($3, $4))
        // 					AND
        // 				( prop2  IN ($3, $4) OR prop IS NULL)
        // 			)
        for (let prop in filter) {
            // if any of these are filtering for NULL we need to add a `OR prop IS NULL`
            const hasNull = filter[prop].includes(null);

            const colAlias = filterColAlias ? `${filterColAlias}.` : '';

            // If we are filtering for a null value, then use IS NULL instead of IN (NULL)`
            let nullSql = '';
            if (hasNull) {
                nullSql = `OR ${colAlias}${prop} IS NULL`;
            }

            filterGroup.push(
                `(
                    ${colAlias}${prop} IN (${this._addParams(filter[prop])})
                    ${nullSql}
              )`
            );
        }
        if (filterGroup.length == 0) {
            return this;
        }
        let filterWhere = `(${filterGroup.join(' AND ')})`;
        return this.where(filterWhere, 'AND');
    }

    rangeFilterMultiColumn(filter: any, filterColAlias?: string): this {
        if (!filter) {
            return this;
        }
        // minCol
        this.config.filter = this.config.filter || {};

        const filterMeta: any = {};
        const colAlias = filterColAlias ? `${filterColAlias}.` : '';

        filterMeta[filter.min.name] = filter.min.value;
        filterMeta[filter.max.name] = filter.max.value;

        Object.assign(this.config.filter, filterMeta); // TODO: attach with name

        const minPlaceholder = this._addParam(filter.min.value);
        const maxPlaceholder = this._addParam(filter.max.value);

        // where minCol between filter.min.value AND filter.max.value
        //
        // OR
        //
        // maxCol between filter.min.value AND filter.max.value
        const filterRange = `
            (
                    ${colAlias}${filter.min.name} BETWEEN ${minPlaceholder} AND ${maxPlaceholder}
                    OR
                    ${colAlias}${filter.max.name} BETWEEN ${minPlaceholder} AND ${maxPlaceholder}
            )
        `;

        return this.where(filterRange, 'AND');
    }

    // Generates a where clause, for rows that match any of the given values, between the givien min and max columns
    //
    // { filter_property: '1,2,3' }
    //
    // (
    // 		WHERE filter.value1 >= MIN_COL AND filter.value1 <= MAX_COL
    // 		 OR
    // 		WHERE filter.value2 >= MIN_COL AND filter.value1 <= MAX_COL
    // 		 OR
    // 		...
    // )
    //
    //
    betweenColumnValuesMulti(filter: { [key: string]: any[] }, minCol: string, maxCol: string, filterColAlias?: string): this {
        this.config.filter = this.config.filter || {};
        Object.assign(this.config.filter, filter);

        const groups: string[] = [];
        const colAlias = filterColAlias ? `${filterColAlias}.` : '';

        for (let property in filter) {
            for (let value of filter[property]) {
                const param = this._addParam(value);

                /// (value >= a.colname AND value <= a.otherColName)
                groups.push(
                    ` (${param} >= ${colAlias}${minCol} AND ${param} <= ${colAlias}${maxCol}) `
                );
            }
        }

        if (groups.length == 0) {
            return this;
        }

        const filterWhere = `(${groups.join(' OR ')})`;

        return this.where(filterWhere, 'AND');
    }

    betweenColumnValues(filter: { name: string; value: any }, minCol: string, maxCol: string, filterColAlias?: string): this {
        if (!(filter && filter.name)) {
            return this;
        }

        this.config.filter = this.config.filter || {};
        this.config.filter[filter.name] = filter.value;
        const colAlias = filterColAlias ? `${filterColAlias}.` : '';
        if (!filter.value) {
            return this;
        }
        this.whereLTE(`${colAlias}${minCol}`, filter.value, 'AND');
        this.whereGTE(`${colAlias}${maxCol}`, filter.value, 'AND');
        return this;
    }

    /**
     * Takes an object with zero or more list of filters to be applied to array columns
     * @return {[type]} [description]
     */
    filterArray(
        filterArrays: { [key: string]: any[] },
        alias?: string,
        matchNull: boolean = false,
        matchEmpty: boolean = false
    ): this {
        this.config.filter = this.config.filter || {};

        Object.assign(this.config.filter, filterArrays);

        for (let prop in filterArrays) {
            this._filterArray(filterArrays[prop], prop, alias, matchNull, matchEmpty);
        }
        return this;
    }

    /**
     * Filters a single column with a list of values, optionally also matching null
     * @param  {string}  column        [description]
     * @param  {string}  filterColAlias [description]
     * @param  {string[]}  filterValues  [description]
     * @param  {Boolean} matchNull     [description]
     * @return {QueryBuilder}                [description]
     */
    private _filterArray(
        filterValues: any[],
        column: string,
        filterColAlias?: string,
        matchNull: boolean = false,
        matchEmpty: boolean = false
    ): this {
        const colname = `${filterColAlias ? `${filterColAlias}.` : ''}${column}`;

        // AND
        // 	(
        // 			(value = ANY (column))
        // 			AND
        // 			(value = ANY (column))
        //
        // 			-- also match null
        // 			OR (
        // 				array_position(column, NULL) is not NULL
        // 				OR
        // 				column is null
        // 			)
        // 	)
        //

        let groups: string[] = [];
        for (let value of filterValues) {
            groups.push(
                `(array_position(${colname}, ${this._addParam(value)}) is not NULL)`
            );
            // groups.push(`(value = ANY (${colname}))`);
        }

        let nullMatchSql = '';
        if (matchNull) {
            nullMatchSql = `OR (
                    (
                            array_position(${colname}, NULL) is not NULL
                        OR
                            ${colname} is NULL
                    )
                )`;
            // groups.push(`(${colname} is NULL)`);
        }

        let emptyMatchSql = '';
        if (matchEmpty) {
            emptyMatchSql = `OR (
                    (

                        -- only match empty arrays
                        ${colname} IS NOT NULL
                            AND
                        -- returns NULL if length is zero
                        array_length(${colname}, 1) IS NULL
                    )
                )`;
            // groups.push(`(${colname} is NULL)`);
        }

        const filterWhere = `(
        (
            \n\t${groups.join(`\n\t AND \n\t`)}\n\t
        )
        ${nullMatchSql}
        ${emptyMatchSql}
        ) `;
        return this.where(filterWhere, 'AND');

        // return this;
    }

    orderBy(column: string, direction?: string, orderColAlias?: string): this {
        this.sorts.push(
            `${orderColAlias ? `${orderColAlias}.` : ''}${column} ${
                direction ? direction : ''
            }`
        );
        return this;
    }

    sort(column: string, direction?: string, sortColAlias?: string): this {
        return this.orderBy(column, direction, sortColAlias);
    }

    private _where(column: string, param: any, jointype?: string, operator: string = '='): this {
        if (this.wheres.length > 0) {
            jointype = jointype || 'AND';
        }
        const clause = `${column} ${operator} ${this._addParam(param)}`;
        this.wheres.push(jointype ? `${jointype} ${clause}` : clause);
        return this;
    }

    private _between(column: string, min: any, max: any, not: boolean = false, jointype?: string): this {
        if (this.wheres.length > 0) {
            jointype = jointype || 'AND';
        }
        const clause = `${column}  ${not ? 'NOT' : ''} BETWEEN ${this._addParam(
            min
        )} AND ${this._addParam(max)}`;
        this.wheres.push(jointype ? `${jointype} ${clause}` : clause);
        return this;
    }

    private _join(table: string, onClause: string, alias?: string, joinType?: string): this {
        this.joins.push(
            `${joinType || ''} JOIN ${table} ${alias ? alias : ''} ON (${onClause})`
        );
        return this;
    }

    // returns the placeholder
    private _addParam(value: any): string {
        this.params.push(value);
        const dialect = getDialect();
        if (dialect === 'mysql') {
            return `?`;
        }
        return `$${this.params.length}`;
    }

    private _addParams(valueArray: any[]): string {
        if (!valueArray) {
            return '';
        }
        let params: string[] = [];
        for (let param of valueArray) {
            params.push(this._addParam(param));
        }
        return params.join(', ');
    }

    format(sql: string): string {
        return sql;
    }

    private _generateSQL(): string {
        const sql = `SELECT\n  ` +
            `${this.columns.join(',\n  ')} \nFROM \n${this.tables.join(',\n  ')}` +
            `${this.joins.join('\n  ')}` +
            `${this.wheres.length > 0 ? ' WHERE\n' : ''}` +
            `${this.wheres.join('\n  ')}` +
            `${this.groups.length ? ` GROUP BY \n ${this.groups.join(',\n  ')}\n` : ``}` +
            `${this.sorts.length ? ` ORDER BY \n ${this.sorts.join(',\n ')}\n` : ``}` +
            `${this._offset ? ` OFFSET ${this._offset}\n` : ''}` +
            `${this._limit ? ` LIMIT ${this._limit}` : ''}`
            +`;`;
        return sql;
    }

    sql(formatted: boolean = true): { sql: string; params: any[] } {
        let sql = this._generateSQL();
        return { sql, params: this.params };
    }

    /**
     * Takes a result set and strips out the PAGINATION_COLS into a combined meta
     * @return {Object} `{ meta: {}, results: {}}`]
     */
    extractPaginatedResults(results: any[]): { meta: any; results: any[] } {
        let paginatedResults = {
            meta: {
                page: undefined,
                per: undefined,
                num_pages: undefined,
                total_items: undefined,
                sortBy: this.config.pagination
                    ? this.config.pagination.sortBy
                    : undefined,
                sortDir: this.config.pagination
                    ? this.config.pagination.sortDir
                    : undefined,
                // pagination: this.config.pagination,
                search: this.config.search,
                filter: this.config.filter,
            },
            results: [] as any[],
        };

        // get pagination data from first result
        if (results.length > 0) {
            let res = results[0];
            paginatedResults.meta.page = res._pagination_page;
            paginatedResults.meta.per = res._pagination_per_page;
            paginatedResults.meta.num_pages = res._pagination_num_pages;
            paginatedResults.meta.total_items = res._pagination_total_items;
        }

        // remove pagination properties
        paginatedResults.results = results.map((result) => {
            result._pagination_page = undefined;
            result._pagination_per_page = undefined;
            result._pagination_num_pages = undefined;
            result._pagination_total_items = undefined;
            return result;
        });

        return paginatedResults;
    }

    findOne(db: DB): Promise<any> {
        let query = this.sql();
        return db.findOne(query.sql, query.params);
    }

    /**
     *
     * @param  {DatabaseService} db a database instance
     * @param  {function} customResultsFunc a function applied to results, before further metadata processing e.g. a custom sort
     */
    async findMany(db: DB, customResultsFunc?: (results: any[]) => void): Promise<any> {
        let query = this.sql();
        let results = await db.findMany(query.sql, query.params).catch((err) => {
            console.error(err);
            throw err;
        });

        if (customResultsFunc) {
            try {
                customResultsFunc(results);
            } catch (err) {
                console.error('could not apply custom results function', err);
            }
        }

        if (this.includeMetadata) {
            return this.extractPaginatedResults(results);
        }
        return results;
    }

    dump(): void {
        console.log(this._generateSQL(), this.params);
    }
}

export default <TSchema extends SchemaDefinition = SchemaDefinition>(): QueryBuilder<TSchema> => {
    return new QueryBuilder<TSchema>();
};
export { QueryBuilder, type SchemaDefinition, type TableSchema, type ColumnType };