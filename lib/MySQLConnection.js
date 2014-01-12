!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, mysql 		= require('mysql')
		, type 			= require('ee-types')
		, async 		= require('ee-async')
		, argv 			= require('ee-argv')
		, moment 		= require('moment')
		, Connection 	= require('../../ee-db-connection')
		, QueryBuilder 	= require('../../ee-query-builder');


	var debug = argv.has('debug-sql');



	module.exports = new Class({
		inherits: Connection


		, _bleedReg: /transaction|declare|set|delimiter|execute/gi
		, _writeReg: /create|insert|delete|update|alter|flush|drop|truncate|call|DELIMITER|execute|DEALLOCATE/gi

		

		/**
		 * class constructor
		 *
		 * @param <Object> connection options
		 */
		, init: function init(options) {
			init.parent(options);

			this._querBuilder = new QueryBuilder({
				  escapeId: this._escapeId
				, escape: 	this._escape
			});
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
				, typeCast: true
			});


			// connect
			this.connection.connect();

			// handle errors
			this.connection.on('error', this._handleConnectionError.bind(this));

			// query the server
			this.connection.query('SELECT 1;', done);
		}



		, _render: function(){
			return this._querBuilder._render.apply(this._querBuilder, Array.prototype.slice.call(arguments));
		}


		, _toString: function(){
			return this._querBuilder._toString.apply(this._toString, Array.prototype.slice.call(arguments));
		}


		, _toType: function(){
			return this._querBuilder._toType.apply(this._toType, Array.prototype.slice.call(arguments));
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
			var start;

			if (debug) {
				log(sql);
				start = Date.now();
			}

			this.connection.query(sql, function(err, results){
				if (debug) log('query took «'+(Date.now()-start)+'» msec ...');
				callback(err, results);
			});
		}


		/**
		 * the _describe() method returns a detailed description of all 
		 * databases, tables and attributes
		 *
		 * @param <Function> callback
		 */
		, _describe: function(callback){
			this.listDatabases(function(err, databases){
				if (err) callback(err);
				else {

					// get definition for each database
					async.each(databases, function(databaseName, next){
						// get relations
						async.wait(function(done){
							this.listContraints(databaseName, done);
						}.bind(this)

						// get table definitions
						, function(done){
							this.describeTables(databaseName, done);
						}.bind(this)

						// clean up results
						, function(err, results){
							if(err) callback(err);
							else {
								next(null, {
									  databaseName: databaseName
									, constraints: 	results[0]
									, tables: 		results[1]
								});
							}
						}.bind(this));
					}.bind(this)

					// reformat definitions
					, function(err, definitions){
						if (err) callback(err);
						else {
							var dbs = {};

							definitions.forEach(function(db){
								var database;

								if (!dbs[db.databaseName]) {
									dbs[db.databaseName] = {};
									Object.defineProperty(dbs[db.databaseName], 'getDatabaseName', {
										value: function(){return db.databaseName;}
									});
								}
								database = dbs[db.databaseName];
								

								// map tables
								db.tables.forEach(function(definition){
									var table;

									if (!database[definition.TABLE_NAME]) {
										database[definition.TABLE_NAME] = {
											  primaryKeys: []
											, isMapping: false
										};

										Object.defineProperty(database[definition.TABLE_NAME], 'getTableName', {
											value: function(){return definition.TABLE_NAME;}
										});
										Object.defineProperty(database[definition.TABLE_NAME], 'getDatabaseName', {
											value: function(){return db.databaseName;}
										});
									}
									table = database[definition.TABLE_NAME];
									
									table[definition.COLUMN_NAME] = {
										  nullable: 	definition.IS_NULLABLE === 'YES'
										, type: 		definition.DATA_TYPE
										, length: 		definition.CHARACTER_MAXIMUM_LENGTH || definition.NUMERIC_PRECISION
										, isPrimary: 	false
										, isUnique: 	false
										, isForeignKey: false
									};
								}.bind(this));

								// map relations
								Object.keys(db.constraints).forEach(function(tableName){
									var   fks 		= []
										, primaries = []
										, uniques 	= [];

									// gather info
									Object.keys(db.constraints[tableName]).forEach(function(constraintName){
										var constraint = db.constraints[tableName][constraintName];

										constraint.rules.forEach(function(rule){
											switch (constraint.type) {
												case 'primary key':
													primaries.push(rule.table_name);
													database[tableName][rule.column_name].isPrimary = true;
													database[tableName].primaryKeys.push(rule.column_name);
													break;

												case 'unique':
													uniques.push(rule.table_name);
													database[tableName][rule.column_name].isUnique = true;
													break;

												case 'foreign key':
													fks.push(rule.table_name);
													database[tableName][rule.column_name].isForeignKey = true;
													database[tableName][rule.column_name].referencedTable = rule.referenced_table_name;
													database[tableName][rule.column_name].referencedColumn = rule.referenced_column_name;
													database[tableName][rule.column_name].referencedModel = database[rule.referenced_table_name][rule.referenced_column_name];
													break;
											}
										});
									}.bind(this));

									// check for mapping, its a mapping if there is a table
									// with two primary key columns which are foreign keys
									// or if there are two unique columns which are foreign keys
									if (primaries.length > 1) {
										var score = 0;
										primaries.forEach(function(column){
											if (fks.indexOf(column) >= 0) score++;
										});

										if (score > 1) database[tableName].isMapping = true;
									}

									if (uniques.length > 1) {
										var score = 0;
										uniques.forEach(function(column){
											if (fks.indexOf(column) >= 0) score++;
										});

										if (score > 1) database[tableName].isMapping = true;
									}

								}.bind(this));
							}.bind(this));

							log(dbs);
														
							callback(null, dbs);
						}
					}.bind(this));
				}
			}.bind(this));
		}



		, listContraints: function(databaseName, callback){
			async.wait(function(done){
				this.query({
					select: ['table_name', 'column_name', 'referenced_table_name', 'referenced_column_name', 'constraint_name']
					, from: {
						  database: 'information_schema'
						, table: 'key_column_usage'
					}
					, filter: {
						constraint_schema: databaseName
					}
					, order: ['table_name', 'column_name']
				}, done);
			}.bind(this), 


			function(done){
				this.query({
					select: ['table_name', 'constraint_type', 'constraint_name']
					, from: {
						  database: 'information_schema'
						, table: 'table_constraints'
					}
					, filter: {
						constraint_schema: databaseName
					}
					, order: ['table_name', 'constraint_name', 'constraint_type']
				}, done);
			}.bind(this)


			, function(err, results){
				if (err) callback(err);
				else {
					var constraints = {}, tables = {};

					// join the separate results
					results[0].forEach(function(constraint){
						if (!constraints[constraint.table_name]) constraints[constraint.table_name] = {};
						if (!constraints[constraint.table_name][constraint.constraint_name]) constraints[constraint.table_name][constraint.constraint_name] = {rules: [], type: 'unknown'};

						constraints[constraint.table_name][constraint.constraint_name].rules.push(constraint);
					});

					results[1].forEach(function(constraint){
						if (!constraints[constraint.table_name]) constraints[constraint.table_name] = {};
						if (!constraints[constraint.table_name][constraint.constraint_name]) constraints[constraint.table_name][constraint.constraint_name] = {rules: []};

						constraints[constraint.table_name][constraint.constraint_name].type = constraint.constraint_type.toLowerCase();
					});

					callback(null, constraints);
				}
			}.bind(this));
		}


		, describeTables: function(databaseName, callback){
			this.query({
				filter: {
					TABLE_SCHEMA: databaseName
				}
				, from: 'information_schema.columns'
				, select: ['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'COLUMN_DEFAULT', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION']
			}, callback);
		}


		, listTables: function(databaseName, callback){
			this._query('SHOW TABLES in '+databaseName+';', callback);
		}


		, listDatabases: function(callback){
			this._query('SHOW DATABASES;', function(err, databases){
				if (err) callback(err);
				else {
					databases = (databases || []).filter(function(row){
						return row.Database !== 'information_schema';
					}).map(function(row){
						return row.Database;
					})

					callback(null, databases);
				}
			}.bind(this));
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
