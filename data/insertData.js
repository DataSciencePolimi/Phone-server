'use strict';

const fs = require('fs');
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;
 
const config = require('../config/configuration.json');

console.log('Reading the csv');
var data = fs.readFileSync('outgoing.csv').toString().split('\n');


let year = 2016;

// js month start from 0
let month = 1;

const mongoUrl = `${config.url}/${config.name}`

MongoClient
.connect(mongoUrl)
.then((db)=>{

  console.log('Parsing the data');

  data  = _(data)
    .map((r)=>{
      return r.split(',');
    })
    .map((d) => {
      let date = new Date(year,month,d[5],d[6]);
      return {
        place:d[0],
        age:d[1],
        type:d[2],
        gender:d[3],
        year:year,
        day:d[5],
        hour:d[6],
        country:d[4],
        month:month,
        date: date,
        weekend: (date.getDay()===6 || date.getDay()===0),
        count:d[7]
      };
    })
    .filter((d)=>{
      if(d.age == 0 || d.type==0 || d.gender==0 || d.country == 0){
        return false;
      }else{
        return true;
      }
    })
    .value();

  console.log('Inserting data in the db');

  var collection = db.collection('calls');

  console.log(data.length);
  collection
  .insertMany(data)
  .then(()=>{
    console.log('Data inserted');
  })
  .catch((e)=>{
    console.log(e);
  });

})
.catch((e)=>{
  console.log(e);
})
