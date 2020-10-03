var express = require('express');
var multer = require('multer');
var cors=require('cors')
var upload = multer({ dest: 'uploads/' });
var app = express()
var amqp = require('amqplib/callback_api');
var winston = require('winston')
const { v1: uuidv1 } = require('uuid');
const mongodb = require('mongodb')
const fs = require('fs');
const { ifError } = require('assert');
const { assert } = require('console');

// set variables for message queue
const queueName = 'demo-queue'

// set variables for mongodb client
const MongoClient = mongodb.MongoClient;
const url = 'mongodb://localhost:27017';
const dbName = 'demo';
const collectionName = 'jobStatus'

// create logger
var logger = winston.createLogger({
  transports: [
    new winston.transports.Console(),
    new winston.transports.File({ filename: 'combined.log' })
  ]
});


// handle cors
app.use('/', cors())

// isit up
app.get('/isitup', function(req, res){
    res.send('itisup');
})

// post upload
app.post('/upload', upload.single('myfile'), function(req, res){
    logger.log({
        level:'info',
        message: 'Success: multipart post call received!'
    })

    //set variables for this job
    var filePath = req.file.path
    var destination = req.file.destination
    var mimeType = req.file.mimetype
    var originalName = req.file.originalname

    //make database record
    var jobId = uuidv1()
    
    //connect to database
    MongoClient.connect(url, function(err, client){
        if (err){
            logger.log({
                level:"info",
                message: "Error: connection to MongoDB failed!"
            })
            throw err;
        }
        var dbo = client.db(dbName)
        var record = {
            "jobId": jobId,
            "jobStatus": 'created',
            "queueName": queueName,
            "filePath": filePath,
            "mimeType": mimeType,
            "originalname": originalName
        }

        dbo.collection(collectionName).insertOne(record, function(err,res){
            if(err){
                logger.log({
                    level:"info",
                    message: "Error: insert to DB failed!"
                })
            }
            logger.log({
                level:"info",
                message: "Success: new job created on DB!"
            })    
        })

        var bucket = new mongodb.GridFSBucket(dbo)
        var newFileName = jobId + '_file'
        var stream = bucket.openUploadStream(newFileName)
        var fileId = stream.id // file object id on gridfs
        
        
        // upload file to GridFS
        fs.createReadStream(filePath).pipe(stream).
        on('error', function(error){
            assert.ifError(error)
        }).
        on('finish', function(){
            logger.log({
                level:"info",
                message: "Success: file uploaded to GridFS"
            })
            
            // update record to database
            record.newFileName = newFileName
            record.fileId = fileId.toString()
            record.jobStatus = 'uploaded'
            var query = {jobId: jobId}
            var newRecord = { $set: record };
            dbo.collection(collectionName).updateOne(query, newRecord, function(err,res){
                if(err){
                    logger.log({
                        level:"info",
                        message: "Error: insert to DB failed!"
                    })
                }
                logger.log({
                    level:"info",
                    message: "Success: new job created on DB!"
                })    
            })

            client.close()
            logger.log({
                level:"info",
                message: "Success: connection to DB closed"
            })
        })

    })


    //make msg for message queue
    var queue = queueName
    var msg = {
        'jobId': jobId,
        'filePath': filePath,
        'destination': destination,
        'mimeType':mimeType,
        'originalName':originalName
        }
    
    //connect to message queue
    amqp.connect('amqp://localhost', function(error0, connection){
        if (error0){
            throw error0
        }
        logger.log({
            level:'info',
            message: 'Success: connection to queue %c built!'
        })
        connection.createChannel(function(error1,channel){
            if (error1){
                throw error1
            }
            logger.log({
                level:'info',
                message: 'Success: connection to channel built!'
            })
            
            var fullpath = __dirname + '/' + req.file.path  //get absolute path of file
            msg['filePath'] = fullpath // update msg object with absolute path
            channel.assertQueue(queue, {
                durable: false
            });
            
            // send to mq
            channel.sendToQueue(queue, Buffer.from(JSON.stringify(msg)));
            logger.log({
                level:'info',
                message: 'Success: job message sent to MQ!'
            });
            logger.log({
                level:'info',
                message: 'Success: message is \''.concat(msg.toString()).concat('\'')
            });

        })
        setTimeout(function() {
            logger.log({
                level:'info',
                message: 'Success: connection to queue closed!'
            })
            connection.close();
            //process.exit(0);
        }, 100);

    });
    //console.log('Body- ' + JSON.stringify(req.body));
    res.send('uploaded')
})

// get status/result
app.get('/status/:id', function(req,res){
    logger.log({
        level:'info',
        message: 'Success: status get call received!'
    })

    let id = req.params.id;
    // connect to databse
    MongoClient.connect(url, function(err,client){
        if (err){
            logger.log({
                level: "info",
                message: "Error: connection to MongoDB failed"
            })
            throw err;
        }
        var dbo = client.db(dbName);
        var query = {"jobId": id}
        // query the result
        dbo.collection(collectionName).findOne(query,function(err, result){
            if(err){
                logger.log({
                    level: "info",
                    message: "Error: can not find job id in MongoDB"
                })            
            }
            client.close()
            res.send(result)
            logger.log({
                level: "info",
                message: "Success: return status call response 200"
            })
        })
    })
    

})

// register http server
var server = app.listen(8081, function(){
    var host = server.address().address
    var port = server.address().port
    
    console.log("app listening at http://%s:%s", host, port)
})
