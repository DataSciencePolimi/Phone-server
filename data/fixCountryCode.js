const fs = require('fs');
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;
const config = require('../config/configuration.json');
const cc = require('./ea2.json');
const mongoUrl = `${config.url}/${config.name}`;


MongoClient
  .connect(mongoUrl)
  .then((db)=>{

      console.log('retrieving data');
      let collectionName = 'incalls';
      
      let collection = db.collection(collectionName)

      collection
      .find()
      .toArray()
      .then((data)=>{
        console.log("Data retrieved")
        
        let toSave = [];
        for (var index = 0; index < data.length; index++) {
            var element = data[index];
            
            var code = cc[element.country];
            if(code){
                element.country = code.join(",");
            }
	    
	    console.log("new elmenent:\n");
            console.log(element); 
            toSave.push(collection
            .update({_id:element._id},element))
        }

        Promise.all(toSave)
        .then(()=>{
            console.log('Done');
        })
       
      })
      .catch((e)=>{
          console.log(e);
      });

  });
