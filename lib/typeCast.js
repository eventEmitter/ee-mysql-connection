!function() {
    'use strict';

    
    var log = require('ee-log');


    module.exports = function(field, next) {
        var value;
        //log(field.type, field.name);

        switch(field.type) {
            case 'TINY':
                value = field.string();
                return value === null ? null : (value == '1');
            case 'SHORT':
            case 'LONG':
            case 'FLOAT':
            case 'INT24':
            case 'YEAR':
                return Number(field.string());
            case 'DATE':
            case 'TIMESTAMP': 
            case 'DATETIME':
            case 'NEWDATE':
                value = field.string();
                return value === null ? null : new Date(value);
            case 'TINY_BLOB':
            case 'MEDIUM_BLOB':
            case 'LONG_BLOB':
            case 'BLOB':
                return field.string();
            case 'DECIMAL':
            case 'DOUBLE':
            case 'LONGLONG':
            case 'VARCHAR':
            case 'BIT':
            case 'NEWDECIMAL':
            case 'ENUM':
            case 'SET':
            case 'VAR_STRING':
            case 'STRING':
            case 'GEOMETRY':
            case 'TIME':
                value = field.string();
                //log(value);
                return value;

            case 'NULL':
                field.buffer();
                return null;

            default:
                log.error('unknwon type conversion!', field.string());
        }


        next();
    };
}();
