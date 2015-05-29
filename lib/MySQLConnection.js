!function(){

    var   Class         = require('ee-class')
        , log           = require('ee-log')
        , mysql         = require('mysql')
        , type          = require('ee-types')
        , RelatedError  = require('related-error')
        , async         = require('ee-async')
        , argv          = require('ee-argv')
        , Connection    = require('related-db-connection')
        , QueryBuilder  = require('related-mysql-query-builder');


    
    var   debug         = argv.has('debug-sql') || process.env.debug_sql === true
        , debugErrors   = argv.has('debug-sql-errors')
        , debugSlow     = argv.has('debug-slow-queries')
        , slowDebugTime = debugSlow && type.string(argv.get('debug-slow-queries')) ? argv.get('debug-slow-queries') : 200;






    module.exports = new Class({
        inherits: Connection


        , _bleedReg: /transaction|declare|set|delimiter|execute/gi
        , _writeReg: /create|insert|delete|update|alter|flush|drop|truncate|call|DELIMITER|execute|DEALLOCATE/gi


        // extract query parameters form a string
        , _paramterizeReg: /\?([a-z0-9_-]+)/gi

        /**
         * class constructor
         *
         * @param <Object> connection options
         */
        , init: function init(options, id) {
            init.super.call(this, options, id);

            this._querBuilder = new QueryBuilder({
                  escapeId: this._escapeId.bind(this)
                , escape:   this._escape.bind(this)
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
                  host              : this.options.host
                , port              : this.options.port
                , user              : this.options.username
                , password          : this.options.password
                , bigNumberStrings  : true
                , supportBigNumbers : true
                , debug             : false
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
              LOCK_READ:        'READ'
            , LOCK_WRITE:       'WRITE'
            , LOCK_EXCLUSIVE:   'WRITE'
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
         * @param <object> query configuration 
         */
        , _query: function(configuration) {
            var _this = this, start, oldLimit;

            if (debug || debugSlow || configuration.debug) start = Date.now();

            if (debugErrors || debug) {
                oldLimit = Error.stackTraceLimit;
                Error.stackTraceLimit = Infinity;
                configuration.stack = new Error('stacktrace');
                Error.stackTraceLimit = oldLimit;
            }


            // call the mysql driver
            this.connection.query(configuration.SQL, configuration.values, function(err, data) {
                if (err) {
                    if (err.code === 'ER_DUP_ENTRY') err = new RelatedError.DuplicateKeyError(err);
                }
                // we've goit th epq query context which is useful
                // for debuggin queries
                _this._queryCallback(this, err, data, start, configuration);
            });
        }




        /**
         * callback called by the query function
         * 
         * @param <Object> the postgress driver context
         * @param <Error> optional error object
         * @param <Object> mysql results object
         * @param <Number> optional timestamp when the query was executed
         * @param <Object> the query configuration
         */
        , _queryCallback: function(mysqlQueryContext, err, data, start, configuration) {
            var time, logStr;


            // debug logging
            if (debug || configuration.debug || (debugSlow && (Date.now()-start) > slowDebugTime) || (debugErrors && err)) {
                // capture query time
                time = Date.now()-start;
                logStr = '[MYSQL]['+this.id+'] ';

                // banner
                log.debug(logStr+this._createDebugBanner(debug || configuration.debug ? 'QUERY DEBUGGER' : 'SLOW QUERY')); 

                // status
                if (err) log.error(logStr+'The query failed: '+err);
                else log.debug(logStr+'Query returned '.grey+((data && data.rows ? data.rows.length : 0)+'').yellow+' rows'.white+' ('.grey+((Date.now()-start)+'').yellow+' msec'.white+') ...'.grey);

                // query
                log.debug(logStr+this._renderSQLQuery(mysqlQueryContext).white);

                // trace
                if (err && configuration.stack) {
                    log.info('Stacktrace:');
                    log(configuration.stack);
                } 

                // end banner
                log.debug(logStr+this._createDebugBanner((debug || configuration.debug ? 'QUERY DEBUGGER' : 'SLOW QUERY'), true));
            }


            // don't care if ther is no callback
            if (configuration.callback) {
                if (err) configuration.callback(err);
                else {
                    if (type.object(data)) {
                        // not an select
                        if (data.affectedRows !== undefined) {
                            // insert
                            configuration.callback(null, {
                                  type: 'id'
                                , id:   data.insertId > 0 ? data.insertId : null
                            });
                        }
                        else configuration.callback(null, data);
                    }
                    else configuration.callback(null, data);
                }
            }
        }





        /*
         * bring the query into the correcto format
         *
         * @param <String> SQL
         * @param <Mixed> object, array, null, undefined query parameters
         */
        , _paramterizeQuery: function(configuration) {
            var match;

            // ew're recycling the regex obejct
            this._paramterizeReg.lastIndex = 0;

            // values array for the parameters
            configuration.values = [];

            // get a list of parameters from the string
            while (match = this._paramterizeReg.exec(configuration.SQL)) {
                // add value
                configuration.values.push(configuration.parameters[match[1]]);

                // replace inside sql string
                configuration.SQL = configuration.SQL.replace(match[0], '?');

                // move the index to the correct location
                this._paramterizeReg.lastIndex += '?'.length-match[0].length;
            }
        }



        /*
         * build a raw sql query from a mysql context
         * 
         * @param <Object> mysql query context
         *
         * @returns <String> full SQL query
         */
        , _renderSQLQuery: function(mysqlQueryContext) {
            return mysqlQueryContext.sql;
        }




        /**
         * the _describe() method returns a detailed description of all
         * databases, tables and attributes
         *
         * @param <Function> callback
         */
        , _describe: function(databases, callback) {

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


                // check if the schema exists
                , function(done){
                    this.schemaExists(databaseName, done);
                }.bind(this)
                

                // clean up results
                , function(err, results) {
                    if(err) callback(err);
                    else {
                        next(null, {
                              databaseName: databaseName
                            , constraints:  results[0]
                            , tables:       results[1]
                            , exists:       results[2]
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
                            Object.defineProperty(dbs[db.databaseName], 'schemaExists', {
                                value: function(){return db.exists;}
                            });
                        }
                        database = dbs[db.databaseName];


                        // map tables
                        db.tables.forEach(function(definition){
                            var table;

                            if (!database[definition.TABLE_NAME]) {
                                database[definition.TABLE_NAME] = {
                                      name          : definition.TABLE_NAME
                                    , primaryKeys   : []
                                    , isMapping     : false
                                    , columns       : {}
                                };

                                Object.defineProperty(database[definition.TABLE_NAME], 'getTableName', {
                                    value: function(){return definition.TABLE_NAME;}
                                });
                                Object.defineProperty(database[definition.TABLE_NAME], 'getDatabaseName', {
                                    value: function(){return db.databaseName;}
                                });
                            }
                            table = database[definition.TABLE_NAME];

                            table.columns[definition.COLUMN_NAME] = this._mapTypes(definition);
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
                                              model         : modelB
                                            , column        : modelB.columns[database[tableName].columns[columns[1]].referencedColumn]
                                            , name          : modelB.name //pluralize.plural(modelB.name)
                                            , via: {
                                                  model     : database[tableName]
                                                , fk        : columns[0]
                                                , otherFk   : columns[1]
                                            }
                                        });

                                        // don't add mappings to myself twice
                                        if (modelB !== modelA) {
                                            modelB.columns[database[tableName].columns[columns[1]].referencedColumn].mapsTo.push({
                                                  model         : modelA
                                                , column        : modelA.columns[database[tableName].columns[columns[0]].referencedColumn]
                                                , name          : modelA.name //pluralize.plural(modelA.name)
                                                , via: {
                                                      model     : database[tableName]
                                                    , fk        : columns[1]
                                                    , otherFk   : columns[0]
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
                this.query({query:{
                      select:   ['table_name', 'column_name', 'referenced_table_name', 'referenced_column_name', 'constraint_name']
                    , database: 'information_schema'
                    , from:     'key_column_usage'
                    , filter: {
                        constraint_schema: databaseName
                    }
                    , order: ['table_name', 'column_name']
                }, callback: done});
            }.bind(this),


            function(done) {
                this.query({query: {
                    select:     ['table_name', 'constraint_type', 'constraint_name']
                    , database: 'information_schema'
                    , from:     'table_constraints'
                    , filter: {
                        constraint_schema: databaseName
                    }
                    , order: ['table_name', 'constraint_name', 'constraint_type']
                }, callback: done});
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



        /*
         * translate mysql type definition to standard orm type definition
         *
         * @param <Object> mysql column description
         *
         * @returns <Object> standardized type object
         */
        , _mapTypes: function(mysqlDefinition) {
            var ormType = {};

            // column identifier
            ormType.name = mysqlDefinition.COLUMN_NAME.trim();



            // type conversion
            switch (mysqlDefinition.DATA_TYPE) {
                case 'int':
                case 'tinyint':
                case 'smallint':
                case 'mediumint':
                case 'bigint':
                    ormType.type            = 'integer';
                    ormType.jsTypeMapping   = 'number';
                    ormType.variableLength  = false;

                    if (mysqlDefinition.EXTRA === 'auto_increment') ormType.isAutoIncrementing = true;
                    else if (type.string(mysqlDefinition.COLUMN_DEFAULT)) ormType.defaultValue = parseInt(mysqlDefinition.COLUMN_DEFAULT, 10);

                    if (mysqlDefinition.DATA_TYPE === 'int') ormType.bitLength = 32;
                    else if (mysqlDefinition.DATA_TYPE === 'tinyint') ormType.bitLength = 8;
                    else if (mysqlDefinition.DATA_TYPE === 'smallint') ormType.bitLength = 16;
                    else if (mysqlDefinition.DATA_TYPE === 'mediumint') ormType.bitLength = 24;
                    else if (mysqlDefinition.DATA_TYPE === 'bigint') ormType.bitLength = 64;
                    break;

                case 'bit':
                    ormType.type            = 'bit';
                    ormType.jsTypeMapping   = 'arrayBuffer';
                    ormType.variableLength  = false;
                    ormType.bitLength       = mysqlDefinition.NUMERIC_PRECISION;
                    break;

                case 'date':
                    ormType.type            = 'date';
                    ormType.jsTypeMapping   = 'date';
                    ormType.variableLength  = false;
                    break;

                case 'character':
                    ormType.type            = 'string';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = false;
                    ormType.length          = mysqlDefinition.CHARACTER_MAXIMUM_LENGTH;
                    break;

                case 'varchar':
                case 'text':
                case 'tinytext':
                case 'mediumtext':
                case 'longtext':
                    ormType.type            = 'string';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = true;
                    ormType.maxLength       = mysqlDefinition.character_maximum_length;
                    break;

                case 'numeric':
                case 'decimal':
                case 'double':
                    ormType.type            = 'decimal';
                    ormType.jsTypeMapping   = 'string';
                    ormType.variableLength  = false;
                    ormType.length          = this._scalarToBits(mysqlDefinition.NUMERIC_PRECISION);
                    break;

                case 'float':
                    ormType.type            = 'float';
                    ormType.jsTypeMapping   = 'number';
                    ormType.variableLength  = false;
                    ormType.bitLength       = (parseInt(mysqlDefinition.NUMERIC_PRECISION, 10) < 24 ) ? 32 : 64;
                    break;

                case 'datetime':
                    ormType.type            = 'datetime';
                    ormType.withTimeZone    = true;
                    ormType.jsTypeMapping   = 'date';
                    break;

                case 'timestamp':
                    ormType.type            = 'datetime';
                    ormType.withTimeZone    = false;
                    ormType.jsTypeMapping   = 'date';
                    break;

                case 'time':
                    ormType.type            = 'time';
                    ormType.withTimeZone    = true;
                    ormType.jsTypeMapping   = 'string';
                    break;
            }



            // is null allowed
            ormType.nullable = mysqlDefinition.IS_NULLABLE === 'YES';

            // autoincrementing?
            if (!ormType.isAutoIncrementing) ormType.isAutoIncrementing = false;

            // has a default value?
              if (type.undefined(ormType.defaultValue)) {
                if (type.string(mysqlDefinition.COLUMN_DEFAULT)) ormType.defaultValue = mysqlDefinition.COLUMN_DEFAULT;
                else ormType.defaultValue = null;
            }

            // will be set later
            ormType.isPrimary       = false;
            ormType.isUnique        = false;
            ormType.isReferenced    = false;
            ormType.isForeignKey    = false;

            // the native type, should not be used by the users, differs for every db
            ormType.nativeType = mysqlDefinition.DATA_TYPE;

            // will be filled later
            ormType.mapsTo          = [];
            ormType.belongsTo       = [];

            return ormType;
        }



        /*
         * compute how many bits (bytes) are required to store a certain scalar value
         */
        , _scalarToBits: function(value) {
            var byteLength = 0;

            value = Array.apply(null, {length: parseInt(value, 10)+1}).join('9');

            while(value/Math.pow(2, ((byteLength+1)*8)) > 1) byteLength++;

            return byteLength*8;
        }



        , describeTables: function(databaseName, callback){
            this.query({query: {
                filter: {
                    TABLE_SCHEMA: databaseName
                }
                , database: 'information_schema'
                , from:     'columns'
                , select: ['TABLE_SCHEMA', 'TABLE_NAME', 'COLUMN_NAME', 'COLUMN_DEFAULT', 'IS_NULLABLE', 'DATA_TYPE', 'CHARACTER_MAXIMUM_LENGTH', 'NUMERIC_PRECISION', 'EXTRA']
            }, callback: callback});
        }


        , listTables: function(databaseName, callback){
            this._query({SQL: 'SHOW TABLES in '+databaseName+';', callback: callback});
        }


        , listDatabases: function(callback){
            this._query({SQL: 'SHOW DATABASES;', callback: function(err, databases){
                if (err) callback(err);
                else {
                    databases = (databases || []).filter(function(row){
                        return row.Database !== 'information_schema';
                    }).map(function(row){
                        return row.Database;
                    })

                    callback(null, databases);
                }
            }.bind(this)});
        }



        /**
         * checks if a given schema exists
         *
         * @param <String> schemanem
         */
        , schemaExists: function(schemaName, callback) {
            this.listDatabases(function(err, schemas) {
                if (err) callback(err);
                else callback(null, !!schemas.filter(function(schema) {return schema === schemaName}).length);
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
