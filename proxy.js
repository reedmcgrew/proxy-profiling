
/*
 * WARNING: This proxy will only work with MySQL clients that wait for data
 * to be received from one request before sending another.
 */

var sys = require("util");
var net = require("net");
var Parser = require('./parser');

process.on("uncaughtException", function(err) {
	sys.puts("exception");
        sys.puts(err);
});

if (process.argv.length != 5) {
        sys.puts("Require the following command line arguments:" +
                " proxy_port service_port service_host");
	sys.puts(" e.g. 9001 80 www.google.com");
        process.exit();
}

var proxyPort = process.argv[2];
var servicePort = process.argv[3];
var serviceHost = process.argv[4];
var num_clients = 0;

function microtime(as_seconds){
	var cur_time = new Date().getTime();
	if(as_seconds){
		return cur_time / 1000;
	}
	else{
		return cur_time;
	}
}

function timestamp(){
	var currentTime = new Date();
	var hour = currentTime.getHours();
	var minute = currentTime.getMinutes();
	var month = currentTime.getMonth() + 1;
	var day = currentTime.getDate();
	var year = currentTime.getFullYear();
	return month + "/" + day + "/" + year + " " + hour + ":" + minute;
}

function is_query_start(data){
	data_str = '' + data;
	//Mysql marks the beginning of a query with one of the following symbols
	return 	data_str[0] === "#" 
		|| data_str[0] === "%"
		|| data_str[0] === "&";
}

function istr_contains(haystack,needle){
	return haystack.toLowerCase().indexOf(needle.toLowerCase()) != -1;
}

function get_query_type(data){
	var query = data + '';
	var query_type = "blank";
	if(query != null && query !== ''){
		if(istr_contains(query,"select")) query_type = "select";
		else if(istr_contains(query,"insert")) query_type = "insert";
		else if(istr_contains(query,"update")) query_type = "update";
		else if(istr_contains(query,"delete")) query_type = "delete";
	}

	return query_type;
}

function print_elapsed(start,stop,request){
	var elapsed = stop - start;
	var query_type = get_query_type(request);
	console.log(query_type+","+microtime()+","+elapsed+","+num_clients+","+request);
}

net.createServer(function (proxySocket) {
	num_clients++;
	var start_time = microtime(true);
	var last_request = '';
        var serviceSocket = new net.Socket();
	var parser = new Parser();
	var data_buffers = new Array();
	var last_packet_type = null;

        serviceSocket.connect(servicePort, serviceHost);

	//Handle Data Coming from Client
        proxySocket.on("data", function (data) {
		if(get_query_type(data) != "blank"){
			start_time = microtime(true);
		}
		last_request += data;
		serviceSocket.write(data);
        });

	//Handle Data Coming from Database
	serviceSocket.on("data", function(data) {
		//Buffer queueing is necessary to make sure that the profiling data is written
		//before another request comes in
		data_buffers.push(data);
		parser.write(data);
        });

	//Log complete packets
	parser.on('packet',function(packet){
		//Determine whether this packet marks the end of a query
		var marks_read_ending = (packet.type == Parser.EOF_PACKET &&
			 		last_packet_type != Parser.FIELD_PACKET);
		var marks_other_ending = packet.type == Parser.OK_PACKET;

		//Log profile times and errors.
		if(marks_read_ending || marks_other_ending){
			print_elapsed(start_time,microtime(true),last_request);
			last_request = '';
			start_time = microtime(true);
		}
		else if(packet.type == Parser.ERROR_PACKET){
			console.log("error,"+last_request);
			last_request = '';
		}

		//Copy buffers to current scope, flush non-local buffers
		var temp_buffers = data_buffers;
		data_buffers = new Array();

		//Write buffers back to client
		for(var i = 0; i < temp_buffers.length; i++){
			proxySocket.write(temp_buffers[i]);
		}
		last_packet_type = packet.type;
	});

	//Handle Closing Sockets
        proxySocket.on("close", function(had_error) {
                serviceSocket.end();
		num_clients--;
	});
        serviceSocket.on("close", function(had_error) {
                proxySocket.end();
        });
}).listen(proxyPort, function(){console.log("Listening on port "+proxyPort)});
