


	var MySQLConnection = require('./')
		, log = require('ee-log');




    var connection = new MySQLConnection({
          host      : '10.80.100.1'
        , username  : 'root'
        , password  : ''
        , port      : 3306
        , database  : 'eventbooster'
    });


    connection.on('load', function(err) {
        log(err);

        connection.describe(['eventbooster'], function(err, description) {
            //log(err, description);
        });
    });

    
