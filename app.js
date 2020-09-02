var express = require('express');
var multer = require('multer');
var cors=require('cors')
var upload = multer({ dest: 'uploads/' });
var app = express()

app.use('/', cors())

app.get('/isitup', function(req, res){
    res.send('itisup');
})

app.post('/upload', upload.single('myfile'), function(req, res){
    console.log('Body- ' + JSON.stringify(req.body));
    res.send('uploaded')
})

var server = app.listen(8081, function(){
    var host = server.address().address
    var port = server.address().port
    console.log("app listening at http://%s:%s", host, port)
})