var express = require('express');
var router = express.Router();
var cassandra = require('cassandra-driver');
var fs = require('fs');

//CONNECT TO CASSANDRA FOR USE FOR THE INDEX MODULE
var client = new cassandra.Client({contactPoints:['127.0.0.1']});
client.connect(function(err,result){
  if(err){
    console.log(err)
  }
  console.log("cassandra is connected");
});

/* QUERIES TO CASSANDRA USING DB CLIENT */
var query_insert_file = "INSERT INTO hw4.imgs (id, filename,contents) VALUES(?,?,textAsBlob(?));";
var query_retrieve_file = "SELECT blobAsText(contents) as contents FROM hw4.imgs WHERE filename = ? ALLOW FILTERING;";

//SEND USER TO HOMEPAGE
router.get('/', function(req, res, next) {
  console.log("encountered user at index /");
  res.render('index', { title: 'Express' });
});

//DEPOSIT USER FILE CONTENTS INTO CASSANDRA DB
router.post('/deposit',function(req,res,next){
  console.log('depositing file:' + req.body.filename + "\tcontents length:" + req.body.contents);
  //check if provided necessary parameters in post body
  if(!req.body.filename || !req.body.contents){
    return res.json({'status':'ERROR', 'message': 'missing necessary parameters'});
  }

  //generate unique id for this insert statement
  var id = cassandra.types.uuid();
  // wrap binary data into json, then convert to text.
  var contentWrapper = JSON.stringify({
    'binarydata' : req.body.contents
  });
  //tell client to execute insert statement, with injected parameters.
  client.execute(query_insert_file,[id, req.body.filename, contentWrapper],function(err,result){
    if(err){
      console.log(err);
      return res.json({'status':'ERROR', 'message': 'deposit failed'});
    }
    console.log('finished successful depositing');
    return res.json({'status':'OK','message':'deposit  successful'});
  })
});



//RETRIEVE THE SPECIFIC FILE FROM CASSANDRA DB. FIRST MATCHING ROW ONLY.
router.get('/retrieve',function(req,res,next){
  console.log("req.query: "+ JSON.stringify(req.query));
  var filename;
  if(req.query.filename){
    filename = req.query.filename;
  }else{
    filename = req.body.filename;
  }
  console.log('retrieving the file: ' + filename);
  //check if params we need are provided.
  if(!filename){
    return res.json({'status':'ERROR','message': 'missing necessary parameters'});
  }
  // else query db for filename
  client.execute(query_retrieve_file,[filename],function(err,result){
    if(err){
      console.log(err);
      return res.json({'status':'ERROR', 'message': 'retrieval failed'});
    }
    //Successfully retrieved a result, process first matching row.
    var retrievedRow = result.rows[0];
    var jsoContents = JSON.parse(retrievedRow.contents); //converted from blob to text in select query.
    const buf = Buffer.from(jsoContents.binarydata);
    var filepath = "/data/"+ filename;

    //log statements
    console.log("extracted retrievedRow from db: " + JSON.stringify(retrievedRow));
    console.log("extracted jsoContents from db: " + JSON.stringify(jsoContents));
    console.log('writing to file:' + filepath);
    console.log("with contents : " + buf.toString());
    fs.writeFile(filepath, buf, 'binary', function(err){
      if(err){
        console.log(err);
        return res.json({"status":"ERROR", "message": "Server could not write to file"})
      }
      console.log('finished writing to file, sent to client');
      return res.sendFile(filepath);
    });
  });
});

module.exports = router;
