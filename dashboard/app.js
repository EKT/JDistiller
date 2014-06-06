// This file is part of JDistiller.
//
// JDistiller is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// JDistiller is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
// You should have received a copy of the GNU General Public License
// along with Foobar. If not, see <http://www.gnu.org/licenses/>.
//
// Copyright National Documentation Centre/NHRF 2012,2013,2014
//
// Software Contributors:
// version 2.0 Michael-Angelos Simos, Panagiotis Stathopoulos
// version 1.0 Chrysostomos Nanakos, Panagiotis Stathopoulos
// version 0.1 Panagiotis Stathopoulos

var zeromq = require("zmq");
var net = require("net");
var fs = require('fs');
var ssloptions = {
  key: fs.readFileSync('certs/privatekey.pem'),
  cert: fs.readFileSync('certs/certificate.pem')
};
var express = require('express')
  , routes = require('./routes');
var app = module.exports = express.createServer(ssloptions);
var net = require('net');
var io = require('socket.io').listen(app, {log: false});

var MemStore = express.session.MemoryStore;

// Express Configuration
app.configure(function(){
  app.set('views', __dirname + '/views');
  app.set('view engine', 'jade');
  app.use(express.bodyParser());
  app.use(express.methodOverride());
  app.use(require('stylus').middleware({ src: __dirname + '/public' }));
  app.use(express.static(__dirname + '/public'));
  app.use(express.cookieParser());
  app.use(express.session({store: MemStore({
    reapInterval: 60000 * 10
  }), secret:'dpool_dashboard'
}));
  app.use(app.router);
});

app.configure('development', function(){
  app.use(express.errorHandler({ dumpExceptions: true, showStack: true }));
});

app.configure('production', function(){
  app.use(express.errorHandler());
});

var TotalLoggedUsers = 0;

app.dynamicHelpers(
  {
    session: function(req, res) {
      return req.session;
    },
    user: function(req, res) {
      return req.session.user;
    },
    totalusers: function(req, res) {
      return TotalLoggedUsers;
    },
    flash: function(req, res) {
      return req.flash();
    }
  }
);

function LoginForm(req,res,next) {
        if(req.session.user) {
                next();
        } else {
                res.redirect('/sessions/new?redirurl=' + req.url);
        }
};

// Routes
var gLogmessage = new Array(100);
var gStatus = '';


app.get('/', LoginForm, routes.index);
app.get('/logserver',LoginForm,routes.logserver);
app.get('/nodeinfo',LoginForm,routes.nodeinfo);
app.get('/statistics',LoginForm,routes.statistics);

var users = require('./users');

app.get('/sessions/new', function(req, res) {
  res.render('login', {locals: {redirurl: req.query.redirurl}, layout: "loginlayout"});
});

app.get('/sessions/destroy', function(req, res) {
  TotalLoggedUsers -= 1;
  delete req.session.user;
  res.redirect('/sessions/new');
});

app.post('/sessions', function(req, res) {
  users.authenticate(req.body.login, req.body.password, function(user) {
    if (user) {
      TotalLoggedUsers += 1;
      req.session.user = user;
      res.redirect(req.body.redirurl || '/');
    } else {
      req.flash('warn', 'Login failed.Please check your username and password.');
      res.render('login', {locals: {redirurl: req.body.redirurl}, layout: "loginlayout"});
    }
  });
});



app.listen(4433);
console.log("Express server listening on port %d in %s mode", app.address().port, app.settings.env);

// Connect to JDistiller log server - retrieve message - transmit it
var zmqlogsocket = zeromq.socket('sub');
zmqlogsocket.connect("tcp://127.0.0.1:5004");
zmqlogsocket.subscribe("");
var gIndex = 99;
zmqlogsocket.on('message', function(data) {
	// Colorize log message
	if(data.toString().indexOf("INFO") > 0)
	{
		msg = "<font color='GREEN'>"+data.toString()+"</font><br>";
	}
	if(data.toString().indexOf("ERROR") > 0)
	{
		msg = "<font color='RED'>"+data.toString()+"</font><br>";
	}
	if(data.toString().indexOf("DEBUG") > 0)
	{
		msg = "<font color='BLUE'>"+data.toString()+"</font><br>";
	}
	if(data.toString().indexOf("WARNING") > 0)
	{
		msg = "<font color='ORANGE'>"+data.toString()+"</font><br>";
	}
	if(gIndex == 0)
	{
		gLogmessage.pop();
		gLogmessage.unshift(msg);
	}
	else {
		gLogmessage[gIndex--] = msg;
	}
	io.sockets.emit('logmessage',gLogmessage.join(''));
});

var dashboardStatus = "running";

setInterval(function() {

var socket = net.createConnection("/tmp/distiller_dash.nodejs.status");
socket.setTimeout(1000);

socket.on('connect',function(connect) {
                socket.write("MASTER_GET_STATUS");
                });

socket.on('data',function(data) {
                if(data.toString() == "OK")
                {
                        dashboardStatus = "running";
                }
		else{
                        dashboardStatus = "down";
		}
                socket.destroy()
                delete socket
                });

socket.on('error',function(err) {
                dashboardStatus = "down";
                });

socket.on("timeout",function() {
                console.log("Cannot communicate with server.");
                socket.destroy();
                delete socket
                dashboardStatus = "down";
                });
},5000);

// Connect to JDistiller dashboard server - retrieve message - transmit it
var zmqnodessocket = zeromq.socket('sub');
zmqnodessocket.connect("tcp://127.0.0.1:5005");
zmqnodessocket.subscribe("");
zmqnodessocket.on('message', function(data) {
	gStatus = JSON.parse(data)
	io.sockets.emit('nodesmessage',gStatus);
	io.sockets.emit('mongodbmessage',gStatus.mongodb_stats);
});

io.sockets.on('connection', function(socket) {
	socket.emit('logmessage', gLogmessage.join(''));
	socket.emit('nodesmessage',gStatus);
	var RT = setInterval( function() {
		return	io.sockets.emit('dashboardStatus',dashboardStatus);},5000);
	socket.emit('mongodbmessage',gStatus.mongodb_stats);
	socket.on("disconnect",function() {
                console.log("Disconnected: from socket");
		clearInterval(RT);
        });
});

