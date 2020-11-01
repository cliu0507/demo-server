var express = require('express');
var multer = require('multer');
var cors=require('cors')
var upload = multer({ dest: 'uploads/' });
var app = express()
var amqp = require('amqplib/callback_api');
var winston = require('winston')
var FormData = require('form-data');
const { v1: uuidv1 } = require('uuid');
const mongodb = require('mongodb')
const fs = require('fs');
var path = require('path');
const { ifError } = require('assert');
const { assert } = require('console');

// set variables for message queue
const queueName = 'demo-queue'
const queueName2 = 'domo-detect'

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


function base64_encode(file) {
    // read binary data
    var bitmap = fs.readFileSync(file);
    // convert binary data to base64 encoded string
    return new Buffer(bitmap).toString('base64');
}

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
        message: 'Success: ----- multipart post call received! -----'
    })

    //set variables for this job
    var filePath = req.file.path
    var destination = req.file.destination
    var mimeType = req.file.mimetype
    var originalName = req.file.originalname

    //make database record
    var jobId = uuidv1()

    //connect to database
    MongoClient.connect(url, {useUnifiedTopology: true}, function(err, client){
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
            "originalName": originalName
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
                        message: "Error: update to DB failed!"
                    })
                }
                logger.log({
                    level:"info",
                    message: "Success: update job status to 'uploaded' on DB!"
                });

                // make msg for message queue
                var queue = queueName
                var msg = {
                    'jobId': jobId,
                    'filePath': filePath,
                    'destination': destination,
                    'mimeType':mimeType,
                    'originalName':originalName
                    }
                client.close()
                logger.log({
                    level:"info",
                    message: "Success: connection to DB closed"
                    })
                
                //connect to message queue
                amqp.connect('amqp://localhost', function(error0, connection){
                    if (error0){
                        throw error0
                    }
                    logger.log({
                        level:'info',
                        message: 'Success: connection to queue built!'
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
                        logger.log({
                            level:'info',
                            message: 'Success: ----- post call completed! -----'
                        })
                    }, 100);

                    });       
                            
                });

            });
        });
    //console.log('Body- ' + JSON.stringify(req.body));
    //send response
    res.setHeader("Content-Type","application/json");
    res.send(jobId)
});


app.post('/domo', upload.single('myfile'), function(req, res){
    logger.log({
        level:'info',
        message: 'Success: ----- multipart post call received! -----'
    })

    //set variables for this job
    var filePath = req.file.path
    var destination = req.file.destination
    var mimeType = req.file.mimetype
    var originalName = req.file.originalname

    //make database record
    var jobId = uuidv1()

    //connect to database
    MongoClient.connect(url, {useUnifiedTopology: true}, function(err, client){
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
            "queueName": queueName2,
            "filePath": filePath,
            "mimeType": mimeType,
            "originalName": originalName
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
                        message: "Error: update to DB failed!"
                    })
                }
                logger.log({
                    level:"info",
                    message: "Success: update job status to 'uploaded' on DB!"
                });

                // make msg for message queue
                var queue = queueName2
                var msg = {
                    'jobId': jobId,
                    'filePath': filePath,
                    'destination': destination,
                    'mimeType':mimeType,
                    'originalName':originalName
                    }
                client.close()
                logger.log({
                    level:"info",
                    message: "Success: connection to DB closed"
                    })
                
                //connect to message queue
                amqp.connect('amqp://localhost', function(error0, connection){
                    if (error0){
                        throw error0
                    }
                    logger.log({
                        level:'info',
                        message: 'Success: connection to queue built!'
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
                        logger.log({
                            level:'info',
                            message: 'Success: ----- post call completed! -----'
                        })
                    }, 100);

                    });       
                            
                });

            });
        });
    //console.log('Body- ' + JSON.stringify(req.body));
    //send response
    res.setHeader("Content-Type","application/json");
    res.send(jobId)
});

// get status/result
app.get('/status/:id', function(req,res){
    logger.log({
        level:'info',
        message: 'Success: ----- status get call received! -----'
    })

    let jobId = req.params.id;
    console.log(jobId)
    console.log(typeof(jobId))
    if(jobId == 'undefined' || jobId == undefined){
        res.status(400)
        res.send('empty job id was provided')
        return   
    }

    // connect to databse
    MongoClient.connect(url, { useUnifiedTopology: true }, function(err,client){
        if (err){
            logger.log({
                level: "info",
                message: "Error: connection to MongoDB failed"
            })
            throw err;
        }
        var dbo = client.db(dbName);
        var query = {"jobId": jobId}
        
        // query the result
        dbo.collection(collectionName).findOne(query,function(err, result){
            if(err){
                logger.log({
                    level: "info",
                    message: "Error: can not find job id in MongoDB"
                })            
            }
            
            let returnRes = {
                "jobId": result["jobId"],
                "mimeType": result["mimeType"],
                "originalName" : result["originalName"],
                "resultFileId": result["resultFileId"],
                "fileId": result["fileId"],
                "jobStatus": result["jobStatus"],
            }

            // formulate a multipart response
            var form = new FormData()
            form.append('jobId', jobId);
            form.append('jobStatus',result['jobStatus']);
            
            // check if result file is ready
            if("resultFileId" in result){
                // if result is ready, attach base64 result in multipart response

                // create folder to store result file
                let downloadDir = path.join(process.cwd(),'downloads',jobId)
                if (!fs.existsSync(downloadDir)){
                    fs.mkdirSync(downloadDir)
                }

                // download the result file
                let resultFilePath = path.join(downloadDir,jobId+'_result')
                var bucket = new mongodb.GridFSBucket(dbo)
                var resultFileId = result["resultFileId"]
                bucket.openDownloadStream(new mongodb.ObjectId(resultFileId))
                .pipe(fs.createWriteStream(resultFilePath)).
                on('error',function(error){
                    logger.log({
                        level: "info",
                        message: "Error: failed to download result file"
                    })
                    res.status(500)
                    res.send('failed to download result file')
                    client.close()
                }).
                on('finish', function(){
                    // respond to client
                    var base64str = base64_encode(resultFilePath)
                    form.append('resultFileBase64', base64str);
                    //form.append('part2', 'part 2 string');
                    res.setHeader("Content-Type","multipart/form-data");
                    //res.setHeader('Content-Type', 'multipart/form-data; boundary='+form.getBoundary());
                    //res.setHeader('Content-Type', 'text/plain');
                    form.pipe(res)
                    //res.send(form)
                    // respond to client
                    client.close()
                })
            }
            else
            {
                
                res.send(returnRes)
                client.close()
            }
            
            logger.log({
                level: "info",
                message: 'Success: ----- status call completed! -----'
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
