var express = require('express');
var multer = require('multer');
var cors=require('cors')
var upload = multer({ dest: 'uploads/' });
var app = express()
var amqp = require('amqplib/callback_api');
var winston = require('winston')
var queueName = 'workflow1'


//create logger
var logger = winston.createLogger({
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'combined.log' })
  ]
});



app.use('/', cors())

app.get('/isitup', function(req, res){
    res.send('itisup');
})

app.post('/upload', upload.single('myfile'), function(req, res){
    logger.log({
        level:'info',
        message: 'Multipart post call received!'
    })
    amqp.connect('amqp://localhost', function(error0, connection){
        if (error0){
            throw error0
        }
        logger.log({
            level:'info',
            message: 'Connection to queue %c built!'
        })
        connection.createChannel(function(error1,channel){
            if (error1){
                throw error1
            }
            logger.log({
                level:'info',
                message: 'Connection to channel built!'
            })
            var queue = queueName
            var msg = {
                'filepath': req.file.path,
                'destination': req.file.destination,
                'mimetype':req.file.mimetype,
                'originalname':req.file.originalname
            }
            var fullpath = __dirname + '/' + req.file.path  //get absolute path of file
            msg['filepath'] = fullpath // update msg object with absolute path
            channel.assertQueue(queue, {
                durable: false
            });
            console.log(JSON.stringify(msg))
            // send to mq
            channel.sendToQueue(queue, Buffer.from(JSON.stringify(msg)));
            logger.log({
                level:'info',
                message: 'Job message sent!'
            });
            logger.log({
                level:'info',
                message: 'Message is \''.concat(msg.toString()).concat('\'')
            });

        })
        setTimeout(function() {
            logger.log({
                level:'info',
                message: 'Connection to queue closed!'
            })
            connection.close();
            //process.exit(0);
        }, 500);

    });
    console.log('Body- ' + JSON.stringify(req.body));
    res.send('uploaded')
})

var server = app.listen(8081, function(){
    var host = server.address().address
    var port = server.address().port
    console.log("app listening at http://%s:%s", host, port)
})
