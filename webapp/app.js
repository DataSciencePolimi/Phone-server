'use strict';

const koa = require('koa');
const cors = require('koa-cors');
const router = require('koa-router')();
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;

const NILS = require('./milan_nils.json');

const MONGO_CONFIG = require('../config/configuration.json');
const IN_COLLECTION = 'incalls';
const OUT_COLLECTION = 'calls';

var app = koa();


// DB STUFF

var db;

var connect = function (config) {
    const mongoUrl = `${config.url}/${config.name}`;
    return MongoClient.connect(mongoUrl);
}

var disconnect = function () {
    return db.close();
}



// UTILITIES

// Create the object for the $match step of the mongo aggregation pipeline
var createMatch = function (details, gender, age, type, period, start, end) {

    var match = {
        place: {
            '$ne': 'Milano'
        }
    };

    if(gender){
        if(gender.indexOf(',')!==-1){
            gender = gender.split(',')
        }
        if(!_.isArray(gender)){
            gender = [gender];
        }

        match.gender = {
            '$in':[]
        };

        for (let i = 0; i < gender.length; i++) {
            let element = gender[i];

            match.gender['$in'].push(element);
            
        }
    }

     if(age){
        if(age.indexOf(',')!==-1){
            age = age.split(',')
        }
        if(!_.isArray(age)){
            age = [age];
        }

        match.age = {
            '$in':[]
        };

        for (let i = 0; i < age.length; i++) {
            let element = age[i];

            match.age['$in'].push(element);
            
        }
    }

    if (type) {
        (type === 'b') ? match.type = 'B' : match.type = 'C';
    }

    if (period) {
        (period === 'we' || period === 'weekends') ? match.weekend = true : match.weekend = false;
    }

    if (start) {
        match.date = {};
        match.date['$gt'] = new Date(start);
    }

    if (end) {
        if (!match.date) match.date = {};
        match.date['$lte'] = new Date(end);
    }

    // Retrieve data related to cities in the province
    if (details === 'city') {
        match.nilId = null;
    // Retreive data related to the nils
    } else {
        match.nilId = {
            '$ne': null
        }
    }
    return match;

}



// MIDDLEWARE

// Enabling CORS
app.use(cors());

// Request log
app.use(function* (next) {
    var start = new Date();
    yield next;
    var ms = new Date() - start;
    console.log('%s %s - %s', this.method, this.url, ms);
});


// ROUTES

// retrieve infomration regarding the other endpoints
router.get('/meta', function* (next) {

    console.log('Connecting to Mongo');
    db = yield connect(MONGO_CONFIG);

    const collection = db.collection(OUT_COLLECTION);

    let min = yield collection.find({}, { _id: 0, date: 1 }).sort({ date: 1 }).limit(1).toArray();
    let max = yield collection.find({}, { _id: 0, date: 1 }).sort({ date: -1 }).limit(1).toArray();
    let ageList = yield collection.distinct("age");
    let detail = ['nil', 'city'];
    let period = ['we','wd'];
    let contract = ['b','c'];
    let gender = ['M','F'];

    let response = {
        min: min[0].date,
        max: max[0].date,
        age: ageList,
        detail: detail,
        contract:contract,
        gender:gender
    };

    this.body = response;
    yield disconnect();


});

// getting the incoming calls
router.get('/incoming', function* (next) {
    var query = this.query;

    var gender = query.gender;
    var age = query.age;
    var type = query.contract;
    var period = query.period;
    var start = query.start;
    var end = query.end;
    var detail = query.detail || 'nil';

    
    var match = createMatch(detail, gender, age, type, period, start, end);

    let aggregation = {
        $group: {
            _id: '$country',
            count: {
                $push: '$place',
            }
        }
    }


    if (detail === 'nil') {
        aggregation['$group'].count['$push'] = '$nilId';
    }

    console.log('Connecting to Mongo');
    db = yield connect(MONGO_CONFIG);

    const collection = db.collection(IN_COLLECTION);

    console.log(match);
    console.log(aggregation);

    let data = yield collection.aggregate([{ $match: match }, aggregation]).toArray();

    data = _(data)
        .map((o) => {
            return {
                from: o._id,
                total: _(o.count).countBy().values().sum(),
                to: _(o.count)
                    .countBy()
                    .map((v, k) => {
                        return {
                            calls: v,
                            id: k
                        }
                    })
                    .value()
            }
        })
        .value();

    var response = {
        start: start || undefined,
        end: end || undefined,
        period: period || undefined,
        contract: type || undefined,
        age: age || undefined,
        gender: gender || undefined,
        detail: detail,
        values: data
    };

    this.body = response;
    yield disconnect();

});

// gettomg outgoing calls
router.get('/outgoing', function* (next) {
    var query = this.query;

    var gender = query.gender;
    var age = query.age;
    var type = query.contract;
    var period = query.period;
    var start = query.start;
    var end = query.end;
    var detail = query.detail || 'nil';

    var match = createMatch(detail, gender, age, type, period, start, end);

    let aggregation = {
        $group: {
            _id: '$place',
            values: {
                '$push': {
                    country: '$country',
                    count: '$count'
                }
            }
        }
    };

    if (detail === 'nil') {
        aggregation['$group']['_id'] = '$nilId';
    }

    console.log('Connecting to Mongo');
    db = yield connect(MONGO_CONFIG);

    const collection = db.collection(OUT_COLLECTION);

    console.log(match);
    console.log(aggregation);

    let data = yield collection.aggregate([{ $match: match }, aggregation]).toArray();

    data = _(data)
        .keyBy('_id')
        .mapValues('values')
        .mapValues((a) => {

            return _(a)
                .groupBy('country')
                .mapValues((e) => {
                    return _(e)
                        .map('count')
                        .map(Number)
                        .sum()
                })
                .map((v, k) => {
                    return {
                        'id': k,
                        'calls': v
                    }
                })
                .value();
        })
        .map((v, k) => {
            return {
                'from': k,
                'total': _(v).sumBy('calls'),
                'to': v
            }
        })
        .value();

    var response = {
        start: start || undefined,
        end: end || undefined,
        period: period || undefined,
        contract: type || undefined,
        age: age || undefined,
        gender: gender || undefined,
        detail: detail,
        values: data
    };


    this.body = response;
    yield disconnect();

});


// SERVER START UP

app
    .use(router.routes())
    .use(router.allowedMethods());

app.listen(3000);
console.log('Koa server listening at port 3000');