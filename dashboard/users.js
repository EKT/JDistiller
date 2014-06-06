var LDAP = require('./node_modules/LDAP/LDAP.js');
var ldap = new LDAP({ uri: 'ldap://172.16.0.101:389', version: 3 });

module.exports.authenticate = function(login, password, callback) {
  if(password.length ==0)
  {
	callback(null);
	return;
  }
  var user = {login: login, password: password, role: 'admin'};
  var bind_options = {
    binddn: "EKTDOM\\"+login,
    password: password
    };


ldap.open(function(err) {
    if (err) {
       console.log('Cannot connect to LDAP server.');
       return;
    }
    });

ldap.simpleBind(bind_options, function(err) {
        if(err){
                ldap.close();
                callback(null);
                return;
        } else {
                ldap.close();
                callback(user);
                return;
        }
   });

};
