!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, mysql 		= require('mysql')
		, type 			= require('ee-types')
		, Connection 	= require('../../ee-db-connection');



	module.exports = new Class({
		inherits: Connection


		, _bleedReg: /transaction|declare|set|delimiter|execute/gi
		, _writeReg: /create|insert|delete|update|alter|flush|drop|truncate|call|DELIMITER|execute|DEALLOCATE/gi

		, _operators: {
			  '=': 	'='
			, '<': 	'<'
			, '>': 	'>'
			, '>=': '>='
			, '<=': '<='
			, '!=': '!='
			, 'equal': '='
			, 'notequal': '!='
			, 'lt': '<'
			, 'gt': '>'
			, 'gte': '>='
			, 'lte': '<='
		}

		, _functions: {
			  'null': 'is null'
			, notNull: 'is not null'
		}


		/**
		 * class constructor
		 *
		 * @param <Object> connection options
		 */
		, init: function init(options) {
			init.parent(options);
		}


		/**
		 * the _connect() method creates the database connection
		 *
		 * @param <Function> done callback
		 */
		, _connect: function(done){
			this.connection = mysql.createConnection({
				  host: 	this.options.host
				, user: 	this.options.username
				, password: this.options.password
			});


			// connect
			this.connection.connect();

			// handle errors
			this.connection.on('error', this._handleConnectionError.bind(this));

			// query the server
			this.connection.query('SELECT 1;', done);
		}


		/**
		 * the _render() method creates an sql query from an object
		 *
		 * @param <Object> query
		 */
		, _render: function(mode, query, parameters){
			var SQLString = '';

			// maybe we get already some parmeters from a parent query ..
			parameters = parameters || {};

			// if dont get a query ...
			query = query || {};


			// render select			
			SQLString += ' SELECT ' + (this._renderSelect(parameters, query.select || []) || 1);

			// from
			SQLString += ' FROM ' + this._escapeId(query.from || 'undefined');

			// render filter (where statement)
			SQLString += ' WHERE ' + (this._renderFilter(parameters, query.filter || {}) || 1);


			return {SQLString: SQLString, parameters: parameters};
		}


		/**
		 * the _renderFilter() method creates an sql where statement from 
		 *
		 * @param <Object> query parameters
		 * @param <Object> select tree
		 */
		, _renderSelect: function(parameters, select) {
			var selects = [];

			select.forEach(function(selector){
				switch (type(selector)) {
					case 'string':
						selects.push(this._escapeId(selector));
						break;

					// subquery
					case 'object':
						selects.push('('+this._render('query', selector, parameters)+')');
						break;

					// functions
					case 'function':

						break;
				}
			}.bind(this));

			return selects.join(', ');
		}


		/**
		 * the _renderFilter() method creates an sql where statement from 
		 *
		 * @param <Object> query parameters
		 * @param <Object> filter tree
		 * @param <String> name of the current property
		 * @param <String> name of the current entity
		 */
		, _renderFilter: function(parameters, filter, property, entity){
			var   items = []
				, id;

			switch (type(filter)) {
				case 'array':
					filter.forEach(function(filterItem){
						items.push(this._renderFilter(parameters, filterItem, (property === '_' ? (entity || property) : (property || entity)) ));
					}.bind(this));
					return items.length ? ( items.length === 1 ? items[0] : '(' + items.join(' OR ') + ')' ) : '';


				case 'object':
					Object.keys(filter).forEach(function(name){
						items.push(this._renderFilter(parameters, filter[name], name, property));
					}.bind(this));
					return items.length ? ( items.length === 1 ? items[0] : '(' + items.join(' AND ') + ')' ): '';


				case 'string':
				case 'number':
				case 'date':
				case 'boolean':
				case 'null':
				case 'undefined':
					id = this._getParameterName(parameters, property);
					parameters[id] = filter;
					return this._escapeId(entity || 'na') + '.' + this._escapeId(property) + ' = ?'+id;

				
				case 'function':
					return this._renderCommand(property, filter(), parameters);


				default: 
					throw new Error('Cannot process the type «'+type(filter)+'» in the MySQL querybuilder!').setName('InvalidTypeException');
			}
		}



		, _renderCommand: function(property, command, parameters){
			var id;

			// comparison
			if (command.operator) {
				id = this._getParameterName(parameters, property);

				// must be a valid operator
				if (!this._operators[command.operator]) throw new Error('Unknown operator «'+command.operator+'»!').setName('InvalidOperatorError');

				// is it a subquery or is it scalar value?
				if (command.query){
					return this._escapeId(property) + ' ' + this._operators[command.operator] + ' (' + this._render('query', command.query, parameters).SQLString +')';
				}
				else {
					parameters[id] = command.value;
					return this._escapeId(property) + this._operators[command.operator] + '?'+id;
				}
			}

			// function
			else if (command.fn){
				if (!this._functions[command.fn]) throw new Error('Unknown function «'+command.fn+'»!').setName('InvalidOperatorError')
				return this._escapeId(property) + ' ' + this._functions[command.fn];
			}
			
			// unknown
			else throw new Error('Unknwon command «'+JSON.stringify(command)+'»!').setName('InvalidTypeException')
		}



		, _getParameterName: function(parameters, name){
			var i = 0;
			while(parameters[name+i]) i++;
			return name+i;
		}



		/**
		 * the _canBleed() securely checks if the sql contains statements which
		 * can bleed into the next queries. if yes the conenction must be 
		 * terminated after the query was executed.
		 *
		 * @param <String> input
		 */
		, _canBleed: function(input){
			this._bleedReg.lastIndex = 0;
			return this._bleedReg.test(input);			
		}



		/**
		 * the _escape() securely escapes values preventing sql injection
		 *
		 * @param <String> input
		 */
		, _escape: function(input){
			return this.connection.escape(input);
		}


		/**
		 * the _escapeId() method escapes a name so it doesnt collide with
		 * reserved keywords
		 *
		 * @param <String> input
		 */
		, _escapeId: function(input){
			return mysql.escapeId(input);
		}


		/**
		 * the _query() method send a query to the rdbms
		 *
		 * @param <String> sql
		 * @param <Function> callback
		 */
		, _query: function(sql, callback) {
			this.connection.query(sql, callback);
		}


		/**
		 * the _describe() method returns a detailed description of all 
		 * databases, tables and attributes
		 *
		 * @param <Function> callback
		 */
		, _describe: function(callback){

		}

		/**
		 * the _toString() converts types to db compatible string types
		 *
		 * @param <Mixed> input
		 */
		, _toString: function(input){

		}

		/**
		 * the _toType() converts db string types to js types
		 *
		 * @param <String> input
		 */
		, _toType: function(input){

		}


		/**
		 * the _handleConnectionError() method handles connection errors
		 *
		 * @param <Error> error, optional
		 */
		, _handleConnectionError: function(err){
			this._diconnected();
			this._end(err);
;		}
	});
}();
