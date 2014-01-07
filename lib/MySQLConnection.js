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
		, _render: function(query){
			var components = {
				  join: 	''
				, where: 	''
				, order: 	''
				, group: 	''
				, limit: 	''
			};

			var   joins = []
				, parameters = {};
				, sql = this._renderFilter(parameters, query.filter);

			log(sql);
		}


		/**
		 * the _renderFilter() method creates an sql where statement from 
		 *
		 * @param <Object> query tree
		 */
		, _renderFilter: function(parameters, filter, property){
			var   items = []
				, id;

			switch (type(filter)) {
				case 'array':
					filter.forEach(function(filterItem){
						items.push(this._renderFilter(parameters, filterItem));
					}.bind(this));
					return '(' + items.join(' OR ') + ')';


				case 'object':
					Object.keys(filter).forEach(function(name){
						items.push(this._renderFilter(parameters, filter[name], name));
					}.bind(this));
					return '(' + items.join(' AND ') + ')';


				case 'string':
				case 'number':
				case 'date':
				case 'boolean':
				case 'null':
				case 'undefined':
					id = this._getParameterName(parameters, property);
					parameters[id] = filter;
					return this._escapeId(property) + ' = ?'+id;

				
				case 'function':
					return this._renderCommand(property, filter(), parameters);


				default: 
					throw new Error('Cannot process the type «'+type(filter)+'» in the MySQL querybuilder!').setName('InvalidTypeException');
			}
		}



		, _renderCommand: function(property, command, parameters){
			var id;

			if (command.operator) {
				id = this._getParameterName(parameters, property);
				parameters[id] = command.value;

				if (!this._operators[command.operator]) throw new Error('Unknown operator «'+comm.operator+'»!').setName('InvalidOperatorError')
				return this._escapeId(property) + this._operators[command.operator] + '?'+id;
			}
			else throw new Error('Unknwon command «'JSON.stringify(command)'»!').setName('InvalidTypeException')
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
		 * the _handleConnectionError() method handles connection errors
		 *
		 * @param <Error> error, optional
		 */
		, _handleConnectionError: function(err){
			this._diconnected();

			if (err.code !== 'PROTOCOL_CONNECTION_LOST'){
				// fatal error
				this._error(err);
			}
;		}
	});
}();
