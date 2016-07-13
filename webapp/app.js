'use strict';
const callingCountries = require('country-data').callingCountries;
const koa = require('koa');
const router = require('koa-router')();
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;

const NILS = require('./milan_nils.json')
const MONGO_CONFIG = require('../config/configuration.json');
const IN_COLLECTION = 'incalls';
const OUT_COLLECTION = 'calls';

var app = koa();

var db;

function connect(config) {
    const mongoUrl = `${config.url}/${config.name}`;
    return MongoClient.connect(mongoUrl);
}

function disconnect() {
    return db.close();
}


app.use(function* (next) {
    var start = new Date();
    yield next;
    var ms = new Date() - start;
    console.log('%s %s - %s', this.method, this.url, ms);
});

var getNilId = function(place){

    let nil =  _(NILS)
    .find((o)=> {
        return o.properties.PLACE === place
    });

    if(nil){
        return nil.properties.ID_PLACE;
    }else{
        return place;
    }
};
var createMatch = function (place, gender, age, type, period, start, end) {

    var match = {};

    (gender && gender.toLowerCase() !== 'b') ? match.gender = gender : '';
    (age) ? match.age = age : '';
    (place) ? match.place = place : '';

    if (type) {
        (type === 'b') ? match.type = 'B' : match.type = 'C';
    }

    if (period) {
        (period === 'we') ? match.weekend = true : match.weekend = false;
    }

    if (start) {
        match.date = {};
        match.date['$gt'] = new Date(start);
    }

    if (end) {
        if (!match.date) match.date = {};
        match.date['$lte'] = new Date(end);
    }

    return match;

}

var getCountry = function(countryCode){

    let country = _(callingCountries.all)
    .find((c)=>{
        return c.countryCallingCodes.indexOf('+'+countryCode)!==-1
    });

    if(country){
        return country.alpha2;
    }else{
        return countryCode;
    }
    
}

router.get('/meta',function*(next){

    console.log('Connecting to Mongo');
    db = yield connect(MONGO_CONFIG);

    const collection = db.collection(OUT_COLLECTION);

    let min = yield collection.find({},{_id:0,date:1}).sort({date: 1}).limit(1).toArray();
    let max = yield collection.find({},{_id:0,date:1}).sort({date: -1}).limit(1).toArray();

    let response = {
        min:min[0].date,
        max:max[0].date
    };

    this.body = response;
    yield disconnect();


});


router.get('/incoming', function* (next) {
    var query = this.query;

    let place = query.place;
    var gender = query.gender;
    var age = query.age;
    var type = query.contract;
    var period = query.period;
    var start = query.start;
    var end = query.end;

    var match = createMatch(place, gender, age, type, period, start, end);

    let aggregation = {
        $group: {
            _id: '$country',
            count: {
                $push: '$place',
            }
        }
    }

    console.log('Connecting to Mongo');
    db = yield connect(MONGO_CONFIG);

    const collection = db.collection(OUT_COLLECTION);

    console.log(match);
    console.log(aggregation);
    let data = yield collection.aggregate([{ $match: match }, aggregation]).toArray();

    data = _(data)
        .map((o) => {
            return {
                from:getCountry(o._id),
                to: _(o.count)
                .countBy()
                .map((v,k)=>{
                    return {
                        calls:v,
                        id:getNilId(k)
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
        values: data
    };

    this.body = response;
    yield disconnect();

});

router.get('/outgoing', function* (next) {
    var query = this.query;

    let place = query.place;
    var gender = query.gender;
    var age = query.age;
    var type = query.contract;
    var period = query.period;
    var start = query.start;
    var end = query.end;

    var match = createMatch(place, gender, age, type, period, start, end);


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

    console.log('Connecting to Mongo');
    db = yield connect(MONGO_CONFIG);

    const collection = db.collection(IN_COLLECTION);

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
                        'id': getCountry(k),
                        'calls': v
                    }
                })
                .value();
        })
        .map((v, k) => {
            return {
                'from': getNilId(k),
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
        values: data
    };


    this.body = response;
    yield disconnect();

});

app
    .use(router.routes())
    .use(router.allowedMethods());

app.listen(3000);
console.log('Koa server listening at port 3000');