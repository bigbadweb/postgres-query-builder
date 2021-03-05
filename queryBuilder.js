class QueryBuilder {
	constructor() {
		this.columns = [];
		this.tables = [];
		this.where = [];
		this.joins = [];
		this.limit;

		this.order = [];

		this.params = [];
	}

	select(column, alias = undefined) {
		this.columns.push(alias ? `${column} AS ${alias}` : column);
		return this;
	}

	from(table, alias = undefined) {
		this.tables.push(alias ? `${table} ${alias}` : table);
		return this;
	}

	wherePlain(where, jointype = undefined) {
		if (this.where.length > 0) {
			jointype = jointype || 'AND';
		}
		this.where.push(`${jointype} ${where}`);
		return this;
	}

	whereEquals(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '=');
	}
	whereNotEquals(column, param, jointype = undefined) {
		return this._where(column, param, jointype, '!=');
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
		this.limit = ` OFFSET ${this._addParam(offset)} LIMIT ${this._addParam(
			limit
		)}`;
		return this;
	}

	orderBy(column, direction = undefined) {
		this.order.push(`${column} ${direction ? direction : ''}`);
		return this;
	}

	_where(column, param, jointype = undefined, operator = '=') {
		if (this.where.length > 0) {
			jointype = jointype || 'AND';
		}
		const clause = `${column} ${operator} ${this._addParam(param)}`;
		this.where.push(jointype ? `${jointype} ${clause}` : clause);
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
		const sql = `SELECT
			${this.columns.join(', ')}
		FROM
			${this.tables.join(', ')}
		${this.joins.join(' ')}
		WHERE
			${this.where.join(' ')}
		${this.limit ? this.limit : ''}
		${this.order.length ? `ORDER BY ${this.order.join(', ')}` : ``}

		`;

		return { sql, params: this.params };
	}
}

module.exports = QueryBuilder;

