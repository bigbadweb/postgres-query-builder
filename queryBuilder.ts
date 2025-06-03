const PAGINATION_COLS = [
    '_pagination_page',
    '_pagination_per_page',
    '_pagination_num_pages',
    '_pagination_total_items',
];

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

class QueryBuilder {
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

    select(column: string, alias?: string): this {
        this.columns.push(alias ? `${column} AS ${alias}` : column);
        return this;
    }

    selectDistinct(column: string, alias?: string): this {
        return this.select(`DISTINCT ${column}`, alias);
    }

    selectDistinctOn(column: string, alias?: string): this {
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
        return this._between(column, min, max, false, jointype);
    }
    whereNotBetween(column: string, min: any, max: any, jointype?: string): this {
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

    pagination(
        pagination?: PaginationAndSort,
        countCol?: string,
        sortColAlias?: string
    ): this {
        this.config.pagination = pagination;
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
        if (this.paginated) {
            return this;
        }
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
        );
        this.select(`COUNT(${countCol}) OVER()::int`, '_pagination_total_items');
        this.includeMetadata = true;
        return this;
    }

    search(search?: Search, searchCols: string[] = [], ignoreCase: boolean = true): this {
        this.config.search = search;
        if (!search || !search.query) {
            return this;
        }
        if (!searchCols) {
            throw new Error('Missing search column');
        }
        if (searchCols && !Array.isArray(searchCols)) {
            searchCols = [searchCols];
        }
        if (searchCols.length == 0) {
            throw new Error('Missing search column');
        }
        const paramName = this._addParam(`%${search.query}%`);
        const likes = searchCols.map((searchCol) => {
            if (ignoreCase) {
                return `LOWER(${searchCol}) LIKE LOWER(${paramName})`;
            }
            return `${searchCol} LIKE ${paramName}`;
        });
        this.where(`(${likes.join(' OR ')})`, 'AND');
        return this;
    }

    filter(filter: Filter, filterColAlias?: string): this {
        this.config.filter = this.config.filter || {};
        Object.assign(this.config.filter, filter);
        let filterGroup: string[] = [];
        for (let prop in filter) {
            const hasNull = filter[prop].includes(null);
            const colAlias = filterColAlias ? `${filterColAlias}.` : '';
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

    betweenColumnValues(
        filter: { name: string; value: any },
        minCol: string,
        maxCol: string,
        filterColAlias?: string
    ): this {
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

    filterArray(
        filterArrays: { [key: string]: any[] },
        alias?: string,
        matchNull: boolean = false
    ): this {
        this.config.filter = this.config.filter || {};
        Object.assign(this.config.filter, filterArrays);
        for (let prop in filterArrays) {
            this._filterArray(filterArrays[prop], prop, alias, matchNull);
        }
        return this;
    }

    private _filterArray(
        filterValues: any[],
        column: string,
        filterColAlias?: string,
        matchNull: boolean = false
    ): this {
        const colname = `${filterColAlias ? `${filterColAlias}.` : ''}${column}`;
        let groups: string[] = [];
        for (let value of filterValues) {
            groups.push(
                `(array_position(${colname}, ${this._addParam(value)}) is not NULL)`
            );
        }
        let nullMatchSql = '';
        if (matchNull) {
            nullMatchSql = `OR (
                    (
                        array_position(${colname}, NULL) is not NULL
                        OR ${colname} is NULL
                    )
                )`;
        }
        const filterWhere = `(
        (
            \n\t${groups.join(`\n\t AND \n\t`)}\n\t
        ) ${nullMatchSql}
        ) `;
        return this.where(filterWhere, 'AND');
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

    private _addParam(value: any): string {
        this.params.push(value);
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
        const sql = `SELECT
              ${this.columns.join(',\n  ')}
            FROM
              ${this.tables.join(',\n  ')}
              ${this.joins.join('\n  ')}
            ${this.wheres.length > 0 ? 'WHERE' : ''}
              ${this.wheres.join('\n  ')}


          ${this.groups.length ? `GROUP BY \n ${this.groups.join(',\n  ')}` : ``}

            ${this.sorts.length ? `ORDER BY \n ${this.sorts.join(',\n ')}` : ``}

            ${this._offset ? `OFFSET ${this._offset}` : ''}
            ${this._limit ? `LIMIT ${this._limit}` : ''}
            `;
        return sql;
    }

    sql(formatted: boolean = true): { sql: string; params: any[] } {
        let sql = this._generateSQL();
        return { sql, params: this.params };
    }

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
                search: this.config.search,
                filter: this.config.filter,
            },
            results: [],
        };

        if (results.length > 0) {
            let res = results[0];
            paginatedResults.meta.page = res._pagination_page;
            paginatedResults.meta.per = res._pagination_per_page;
            paginatedResults.meta.num_pages = res._pagination_num_pages;
            paginatedResults.meta.total_items = res._pagination_total_items;
        }

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

    async findMany(db: DB): Promise<any> {
        let query = this.sql();
        let results = await db.findMany(query.sql, query.params).catch((err) => {
            console.error(err);
            throw err;
        });
        if (this.includeMetadata) {
            return this.extractPaginatedResults(results);
        }
        return results;
    }

    dump(): void {
        console.log(this._generateSQL(), this.params);
    }
}

export default (): QueryBuilder => {
    return new QueryBuilder();
};
export { QueryBuilder };