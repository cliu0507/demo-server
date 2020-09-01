var express = require('express')
var app = express()

app.get('/isitup', function(req, res){
    res.send('itisup');
})

var server = app.listen(8081, function(){
    var host = server.address().address
    var port = server.address().port
    console.log("app listening at http://%s:%s", host, port)
})