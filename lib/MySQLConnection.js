!function(){

	var   Class 		= require('ee-class')
		, log 			= require('ee-log')
		, mysql 		= require('mysql')
		, type 			= require('ee-types')
		, async 		= require('ee-async')
		, argv 			= require('ee-argv')
		, moment 		= require('moment')
		, typeCast 		= require('./typeCast')
		, Connection 	= require('ee-db-connection') //*/require('../../ee-db-connection')
		, QueryBuilder 	= require('ee-mysql-query-builder'); //*/require('../../ee-mysql-query-builder');


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
			init.super.call(this, options);

			this._querBuilder = new QueryBuilder({
				  escapeId: this._escapeId.bind(this)
				, escape: 	this._escape.bind(this)
                , type:     'mysql'
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
				, port: 	this.options.port
				, user: 	this.options.username
				, password: this.options.password
				, typeCast: typeCast
				, debug: false
			});


			// connect
			this.connection.connect();

			// handle errors
			this.connection.on('error', this._handleConnectionError.bind(this));

			// query the server
			this.connection.query('SELECT 1;', done);

			// close when the end method was fired
			this.on('end', function(){
                if (this.connection) {
                    this.connection.end();
                    this.connection = null;
                }
            }.bind(this));
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


        /*
         * LOCK_READ:        READ
         * LOCK_WRITE:       WRITE
         * LOCK_EXCLUSIVE:   WRITE
         */
        , _lockModes: {value:{
              LOCK_READ:       	'READ'
            , LOCK_WRITE:      	'WRITE'
            , LOCK_EXCLUSIVE:  	'WRITE'
        }}


        /*
         * st a lock on a tblae
         */
        , lock: function(schema, table, lockType, callback) {
            if (!this._lockModes[lockType]) callback(new Error('Invalid or not supported lock mode «'+lockType+'»!'));

            this._query('LOCK TABLES '+(schema? this._escapeId(schema)+'.': '')+this._escapeId(table)+' '+this._lockModes[lockType]+';', callback);
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


			this.connection.query(sql, function(err, results) { 
				if (debug){
					log.debug('query took «'+(Date.now()-start)+'» msec ...');
					if (results && results.length !== undefined) log.debug('query returned «'+(results ? (results.length || 0) : 0 )+'» rows ...');
					if (results && results.affectedRows !== undefined) log.debug('query affected «'+(results ? (results.affectedRows || 0) : 0 )+'» rows ...');
					log(err);
				}

				if (err) callback(err);
				else {
					if (type.object(results)) {
						// not an select
						// if (results.insertId){
						if (results.affectedRows !== undefined) {
							// insert
							callback(null, {
								  type: 'id'
								, id:  	results.insertId > 0 ? results.insertId : null
							});
						}
						else callback(null, results);
					}
					else callback(null, results);
				}
			});
		}


		/**
		 * the _describe() method returns a detailed description of all
		 * databases, tables and attributes
		 *
		 * @param <Function> callback
		 */
		, _describe: function(databases, callback){

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
			, function(err, definitions) {
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
									  name 			: definition.TABLE_NAME
									, primaryKeys 	: []
									, isMapping 	: false
									, columns 		: {}
								};

								Object.defineProperty(database[definition.TABLE_NAME], 'getTableName', {
									value: function(){return definition.TABLE_NAME;}
								});
								Object.defineProperty(database[definition.TABLE_NAME], 'getDatabaseName', {
									value: function(){return db.databaseName;}
								});
							}
							table = database[definition.TABLE_NAME];

							table.columns[definition.COLUMN_NAME] = {
								  name: 	 	definition.COLUMN_NAME
								, type: 		definition.DATA_TYPE
								, length: 		definition.CHARACTER_MAXIMUM_LENGTH || definition.NUMERIC_PRECISION
								, nullable: 	definition.IS_NULLABLE === 'YES'
								, isPrimary: 	false
								, isUnique: 	false
								, isForeignKey: false
								, isReferenced: false
								, mapsTo: 		[]
								, belongsTo: 	[]
							};
						}.bind(this));


						// map constraints
						Object.keys(db.constraints).forEach(function(tableName){

							// gather info
							Object.keys(db.constraints[tableName]).forEach(function(constraintName){
								var   constraint = db.constraints[tableName][constraintName];

								constraint.rules.forEach(function(rule){
									switch (constraint.type) {
										case 'primary key':
											database[tableName].columns[rule.column_name].isPrimary = true;
											database[tableName].primaryKeys.push(rule.column_name);
											break;

										case 'unique':
											database[tableName].columns[rule.column_name].isUnique = true;
											break;

										case 'foreign key':
											database[tableName].columns[rule.column_name].isForeignKey = true;
											database[tableName].columns[rule.column_name].referencedTable = rule.referenced_table_name;
											database[tableName].columns[rule.column_name].referencedColumn = rule.referenced_column_name;
											database[tableName].columns[rule.column_name].referencedModel = database[rule.referenced_table_name];

											// tell the other side its referenced
											database[rule.referenced_table_name].columns[rule.referenced_column_name].belongsTo.push({
												  targetColumn: rule.column_name
												, name: tableName
												, model: database[tableName]
											});
											database[rule.referenced_table_name].columns[rule.referenced_column_name].isReferenced = true;
											break;
									}
								});
							}.bind(this));


							Object.keys(db.constraints[tableName]).forEach(function(constraintName){
								var   constraint = db.constraints[tableName][constraintName];

								// check for mapping table
								// a rule must have two memebers and may be of type primary
								// or unique. if this rule has fks on both column we got a mapping table
								if (constraint.rules.length === 2 && (constraint.type === 'primary key' || constraint.type === 'unique')){
									var columns = constraint.rules.map(function(rule){ return rule.column_name; });

									// serach for fks on both columns, go through all rules on the table, look for a fk constraint
									if (Object.keys(db.constraints[tableName]).filter(function(checkContraintName){
												var checkConstraint = db.constraints[tableName][checkContraintName];

												return checkConstraint.type === 'foreign key' && (checkConstraint.rules.filter(function(checkRule){
													return columns.indexOf(checkRule.column_name) >= 0;
												})).length === 1;
											}).length === 2){

										database[tableName].isMapping = true;
										database[tableName].mappingColumns = columns;

										// set mapping reference on tables
										var   modelA = database[tableName].columns[columns[0]].referencedModel
											, modelB = database[tableName].columns[columns[1]].referencedModel;

										modelA.columns[database[tableName].columns[columns[0]].referencedColumn].mapsTo.push({
											  model 		: modelB
											, column 		: modelB.columns[database[tableName].columns[columns[1]].referencedColumn]
											, name 			: modelB.name //pluralize.plural(modelB.name)
											, via: {
												  model 	: database[tableName]
												, fk 		: columns[0]
												, otherFk 	: columns[1]
											}
										});

										// don't add mappings to myself twice
										if (modelB !== modelA) {
											modelB.columns[database[tableName].columns[columns[1]].referencedColumn].mapsTo.push({
												  model 		: modelA
												, column 		: modelA.columns[database[tableName].columns[columns[0]].referencedColumn]
												, name 			: modelA.name //pluralize.plural(modelA.name)
												, via: {
													  model 	: database[tableName]
													, fk 		: columns[1]
													, otherFk 	: columns[0]
												}
											});
										}
									}
								}
							}.bind(this));
						}.bind(this));
					}.bind(this));

					callback(null, dbs);
				}
			}.bind(this));
		}



		, listContraints: function(databaseName, callback){
			async.wait(function(done){
				this.query({
					  select: 	['table_name', 'column_name', 'referenced_table_name', 'referenced_column_name', 'constraint_name']
					, database: 'information_schema'
					, from: 	'key_column_usage'
					, filter: {
						constraint_schema: databaseName
					}
					, order: ['table_name', 'column_name']
				}, done);
			}.bind(this),


			function(done) {
				this.query({
					select: 	['table_name', 'constraint_type', 'constraint_name']
					, database: 'information_schema'
					, from: 	'table_constraints'
					, filter: {
						constraint_schema: databaseName
					}
					, order: ['table_name', 'constraint_name', 'constraint_type']
				}, done);
			}.bind(this)


			, function(err, results) {
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
				, database: 'information_schema'
				, from: 	'columns'
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
			this._disconnected();
			this._end(err);
		}
	});
}();
