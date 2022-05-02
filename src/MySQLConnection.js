const mysql = require('mysql');
const type = require('ee-types');
const RelatedError = require('related-error');
const Connection = require('related-db-connection');




module.exports = class MySQLConnection extends Connection {



    // brand name used for logging
    brand = 'MYSQL';



    /*
    * LOCK_READ:        READ
    * LOCK_WRITE:       WRITE
    * LOCK_EXCLUSIVE:   WRITE
    */
    _lockModes = {
        LOCK_READ: 'READ',
        LOCK_WRITE: 'WRITE',
        LOCK_EXCLUSIVE: 'WRITE',
    }




    /**
     * the _connect() method creates the database connection
     *
     * @param <Function> done callback
     */
    driverConnect(config, callback) {

        if (!config.port) config.port = 3306;
        if (!config.username) config.username = 'root';

        this.connection = mysql.createConnection({
                bigNumberStrings  : true
            , supportBigNumbers : true
            , debug             : false
            , user              : config.username
            , password          : config.password
            , host              : config.host
            , port              : config.port
        });


        // connect
        this.connection.connect();


        // handle errors
        this.connection.on('error', (err) => {
    
            // since the conenciton probably ended 
            // anyway we are going to kill it off
            this.connection.end();
            delete this.connection;

            // emit the error event, its used by super
            // to inddicate theat no query is running 
            // anymore
            this.emit('error', err);

            // call the super end method
            this.end(err);
        });



        // query the server
        this.connection.query('SELECT 1;', (err) => {
            callback(err);
        });
    }








    /**
     * ends the connection
     */
    endConnection(callback) {
        this.connection.end(callback);
    }






    /*
        * st a lock on a tblae
        */
    lock(schema, table, lockType, callback) {
        if (!this._lockModes[lockType]) callback(new Error('Invalid or not supported lock mode «'+lockType+'»!'));

        this.query('LOCK TABLES '+(schema? this._escapeId(schema)+'.': '')+this._escapeId(table)+' '+this._lockModes[lockType]+';').then((data) => {
            callback(null, data);
        }).catch(callback);
    }





    /**
     * the _escape() securely escapes values preventing sql injection
     *
     * @param <String> input
     */
    escape(input){
        return this.connection.escape(input);
    }


    /**
     * the _escapeId() method escapes a name so it doesnt collide with
     * reserved keywords
     *
     * @param <String> input
     */
    escapeId(input){
        return mysql.escapeId(input);
    }




    /**
     * the _query() method send a query to the rdbms
     *
     * @param <Object> query configuration
     */
    executeQuery(queryContext) {
        return new Promise((resolve, reject) => {

            this.connection.query(queryContext.sql, queryContext.values, (err, data) => {
                if (err && err.code === 'ER_DUP_ENTRY') err = new RelatedError.DuplicateKeyError(err);

                if (err) reject(err);
                else {
                    if (queryContext.ast) resolve(data);
                    else if (type.object(data)) {
                        // not an select
                        if (data.affectedRows !== undefined) {
                            // insert
                            resolve(data.insertId || null);
                        }
                        else resolve(data);
                    }
                    else resolve(data);
                }
            });
        });
    }





    /*
        * build a raw sql query from a pg context
        *
        * @param <Object> pq query context
        *
        * @returns <String> full SQL query
        */
    renderSQLQuery(sql, values) {
        var   sql       = sql || ''
            , values    = values || []
            , reg       = /\?/gi
            , index     = 0
            , match;

        while (match = reg.exec(sql)) {
            if (values.length > index) {
                sql = sql.replace(match[0], this.escape(values[index]));
            }

            // adjust regexp
            reg.lastIndex += this.escape(values[index]).length-match[0].length;

            index++;
        }

        return sql;
    }
}
