const PAGINATION_COLS = [
	'_pagination_page',
	'_pagination_per_page',
	'_pagination_num_pages',
	'_pagination_total_	items',
];

class QueryBuilder {
	constructor() {
		this.paginated = false;
		this.columns = [];
		this.tables = [];
		this.wheres = [];
		this.joins = [];
		this.limit;

		this.sorts = [];

		this.params = [];
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
		}
		this.wheres.push(`${jointype} ${where}`);
		return this;
	}

	whereEquals(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '=');
	}
	whereNotEquals(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '!=');
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
		return this._where(column, param, jointype, '<');
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
		this.limit = `OFFSET ${this._addParam(offset)} LIMIT ${this._addParam(
			limit
		)}`;
		return this;
	}

	/**
	 * Takes a pagination object as per the pagination middleware
	 * @param  {PaginationAndSort} pagination
	 * @return {QueryBuilder}
	 */
	pagination(pagination = undefined, countCol = undefined) {
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
			this.sort(pagination.sortBy, pagination.sortDir || null);
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

		return this;
	}

	search(search = undefined, searchCols = [], ignoreCase = true) {
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

	sort(column, direction = undefined) {
		this.sorts.push(`${column} ${direction ? direction : ''}`);
		return this;
	}

	_where(column, param, jointype = undefined, operator = '=') {
		if (this.wheres.length > 0) {
			jointype = jointype || 'AND';
		}
		const clause = `${column} ${operator} ${this._addParam(param)}`;
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

	sql() {
		const sql = `
SELECT
  ${this.columns.join(',\n  ')}
FROM
  ${this.tables.join(',\n  ')}
  ${this.joins.join('\n  ')}
WHERE
  ${this.wheres.join('\n  ')}
${this.sorts.length ? `ORDER BY \n ${this.sorts.join(',\n ')}` : ``}
${this.limit ? this.limit : ''}
`;

		return { sql, params: this.params };
	}
}

module.exports = () => {
	return new QueryBuilder();
};
module.exports.QueryBuilder = QueryBuilder;
