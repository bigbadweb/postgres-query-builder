const PAGINATION_COLS = [
	'_pagination_page',
	'_pagination_per_page',
	'_pagination_num_pages',
	'_pagination_total_	items',
];

class QueryBuilder {
	constructor() {
		this.paginated = false;
		this.includeMetadata = false;
		this.columns = [];
		this.tables = [];
		this.wheres = [];
		this.groups = [];
		this.joins = [];
		this._limit;
		this._offset;
		this.sorts = [];
		this.params = [];

		/**
		 * Stores pagination, search and filter config
		 */
		this.config = { filter: null };
	}

	select(column, alias = undefined) {
		this.columns.push(alias ? `${column} AS ${alias}` : column);
		return this;
	}

	selectDistinct(column, alias = undefined) {
		return this.select(`DISTINCT ${column}`, alias);
	}

	selectDistinctOn(column, alias = undefined) {
		return this.select(`DISTINCT ON (${column})`, alias);
	}

	from(table, alias = undefined) {
		this.tables.push(alias ? `${table} ${alias}` : table);
		return this;
	}

	where(where, jointype = undefined) {
		if (this.wheres.length > 0) {
			jointype = jointype || 'AND';
		} else {
			// no jointype if no other wheres
			jointype = '';
		}
		this.wheres.push(`${jointype ? jointype : ''} ${where}`);
		return this;
	}

	whereEquals(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '=');
	}
	whereNotEquals(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '!=');
	}

	whereIsTrue(column, jointype = undefined) {
		return this._where(column, true, jointype, '=');
	}
	whereIsFalse(column, jointype = undefined) {
		return this._where(column, false, jointype, '=');
	}

	whereLike(column, param, jointype = undefined, ignoreCase = true) {
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

	whereIsNot(column, param, jointype = undefined) {
		return this._where(column, param, jointype, ' IS NOT ');
	}

	whereGT(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '>');
	}
	whereGTE(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '>=');
	}
	whereLT(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '<');
	}
	whereLTE(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '<=');
	}

	whereBetween(column, min, max, jointype = undefined) {
		// WHERE column BETWEEN min AND max
		return this._between(column, min, max, false, jointype);
	}
	whereNotBetween(column, min, max, jointype = undefined) {
		// WHERE column NOT BETWEEN min AND max
		return this._between(column, min, max, true, jointype);
	}

	whereIncludes(column, valueList, jointype = undefined) {
		return this.where(`${column} IN (${this._addParams(valueList)})`, jointype);
	}

	groupBy(column) {
		this.groups.push(column);
		return this;
	}

	left_join(table, onClause, alias = undefined) {
		return this._join(table, onClause, alias, 'LEFT');
	}
	right_join(table, onClause, alias = undefined) {
		return this._join(table, onClause, alias, 'RIGHT');
	}

	join(table, onClause, alias = undefined) {
		return this._join(table, onClause, alias);
	}

	page(page, per) {
		const limit = per;
		const offset = (page - 1) * per;

		this.params.push();
		this._limit = limit;
		this._offset = offset;

		return this;
	}

	limit(limit) {
		this._limit = this._addParam(limit);
	}

	offset(offset) {
		this._offset = this._addParam(offset);
	}

	/**
	 * Takes a pagination object as per the pagination middleware
	 * @param  {PaginationAndSort} pagination
	 * @countCol {string} the column to use as an overall count
	 * @return {QueryBuilder}
	 */
	pagination(
		pagination = undefined,
		countCol = undefined,
		sortColAlias = undefined
	) {
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
			this.sort(pagination.sortBy, pagination.sortDir || null, sortColAlias);
		}

		if (pagination.includeMetadata && countCol) {
			this.paginationCounts(countCol, pagination);
		}

		this.paginated = true;

		return this;
	}

	paginationCounts(countCol, pagination) {
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

	search(search = undefined, searchCols = [], ignoreCase = true) {
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
			return this;
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
	 * @typedef Filter
	 * @property {string} filter_column - the column to filter on
	 * @property {string[]} filter_values - an array of values to filter on
	 */

	/**
	 * Generate filter SQL for the given object
	 * @param  {Filter} filter         the filter configuration
	 * @param  {string} filterColAlias the alias of the table that filters should be applied when there is ambiguity
	 * @return {[type]}                [description]
	 */
	filter(filter, filterColAlias = undefined) {
		this.config.filter = this.config.filter || {};
		Object.assign(this.config.filter, filter);
		let filterGroup = [];

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

	betweenColumnValues(filter, minCol, maxCol, filterColAlias = undefined) {
		if (!(filter && filter.name)) {
			return this;
		}

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
	filterArray(filterArrays, alias, matchNull = false) {
		this.config.filter = this.config.filter || {};

		Object.assign(this.config.filter, filterArrays);

		for (let prop in filterArrays) {
			this._filterArray(filterArrays[prop], prop, alias, matchNull);
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
	_filterArray(
		filterValues,
		column,
		filterColAlias = undefined,
		matchNull = false
	) {
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

		let groups = [];
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
						OR ${colname} is NULL
					)
				)`;
			// groups.push(`(${colname} is NULL)`);
		}

		const filterWhere = `(
		(
			\n\t${groups.join(`\n\t AND \n\t`)}\n\t
		) ${nullMatchSql}
		) `;
		return this.where(filterWhere, 'AND');

		// return this;
	}

	orderBy(column, direction = undefined, orderColAlias = undefined) {
		this.sorts.push(
			`${orderColAlias ? `${orderColAlias}.` : ''}${column} ${
				direction ? direction : ''
			}`
		);
		return this;
	}

	sort(column, direction = undefined, sortColAlias = undefined) {
		return this.orderBy(column, direction, sortColAlias);
	}

	_where(column, param, jointype = undefined, operator = '=') {
		if (this.wheres.length > 0) {
			jointype = jointype || 'AND';
		}
		const clause = `${column} ${operator} ${this._addParam(param)}`;
		this.wheres.push(jointype ? `${jointype} ${clause}` : clause);
		return this;
	}

	_between(column, min, max, not = false, jointype = undefined) {
		if (this.wheres.length > 0) {
			jointype = jointype || 'AND';
		}
		const clause = `${column}  ${not ? 'NOT' : ''} BETWEEN ${this._addParam(
			min
		)} AND ${this._addParam(max)}`;
		this.wheres.push(jointype ? `${jointype} ${clause}` : clause);
		return this;
	}

	_join(table, onClause, alias = undefined, joinType = undefined) {
		this.joins.push(
			`${joinType || ''} JOIN ${table} ${alias ? alias : ''} ON (${onClause})`
		);
		return this;
	}

	// returns the placeholder
	_addParam(value) {
		this.params.push(value);
		return `$${this.params.length}`;
	}

	_addParams(valueArray) {
		if (!valueArray) {
			return;
		}
		let params = [];
		for (let param of valueArray) {
			params.push(this._addParam(param));
		}
		return params;
	}

	format(sql) {
		return sql;
	}

	_generateSQL() {
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

	sql(formatted = true) {
		let sql = this._generateSQL();
		return { sql, params: this.params };
	}

	/**
	 * Takes a result set and strips out the PAGINATION_COLS into a combined meta
	 * @return {Object} `{ meta: {}, results: {}}`]
	 */
	extractPaginatedResults(results) {
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
			results: [],
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

	findOne(db) {
		let query = this.sql();
		return db.findOne(query.sql, query.params);
	}

	async findMany(db) {
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

	dump() {
		console.log(this._generateSQL(), this.params);
	}
}

module.exports = () => {
	return new QueryBuilder();
};
module.exports.QueryBuilder = QueryBuilder;
