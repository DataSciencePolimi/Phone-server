'use strict';

const fs = require('fs');
const _ = require('lodash');
const MongoClient = require('mongodb').MongoClient;
const callingCountries = require('country-data').callingCountries;

const US_COUNTRYCODES = require('./us_country_codes.json');
const CANADA_COUNTRYCODES = require('./canada_country_codes.json');
const config = require('../config/configuration.json');
const NILS = require('./milan_nils.json')
const COLLECTION = 'calls';
const FILE = 'outgoing.csv'

console.log('Reading the csv');
var data = fs.readFileSync(FILE).toString().split('\n');


let year = 2016;

// js month start from 0
let month = 1;

const mongoUrl = `${config.url}/${config.name}`

var getNilId = function (place) {

  let nil = _(NILS)
    .find((o) => {
      return o.properties.PLACE === place
    });

  if (nil) {
    return nil.properties.ID_PLACE;
  } else {
    return null;
  }
};

// Retrieve the country ISO-2 code from the phone code
var getCountry = function (countryCode) {


  // Return 0 if no countrycode is passed
  if (!countryCode) {
    return [0, null];
  }

  // Normal case
  let country = _(callingCountries.all)
    .find((c) => {
      return c.countryCallingCodes.indexOf('+' + countryCode) !== -1
    });

  if (country) {
    return [country.alpha2, null];
  }

  // Long country code case (i.e. Dominican Republic)
  if (countryCode.length >= 4) {
    let longCode = countryCode[0] + ' ' + countryCode.substring(1, countryCode.length);

    let country = _(callingCountries.all)
      .find((c) => {
        return c.countryCallingCodes.indexOf('+' + longCode) !== -1
      });

    if (country) {
      return [country.alpha2, null]
    }
  }

  // North America number notation
  if (countryCode[0] == 1) {
    // Check if it's a USA state
    let naCountryCode = countryCode.substring(1, countryCode.length);

    let usCountry = US_COUNTRYCODES[naCountryCode];

    if (usCountry) {
      // Return US, in the future return the actual country
      return ['US', usCountry];
    } else {
      // Check if it's a Canadian province
      let canadaRegion = CANADA_COUNTRYCODES[naCountryCode];

      if (canadaRegion) {
        // In the future return the actual region
        return ['CA', canadaRegion];
      } else {
        return [countryCode, null];
      }
    }
  } else if (countryCode[0] == 7) {
    // Russian number notation

    // 76x Khazakhistan
    if (countryCode[1] == 6) {
      return ['KZ', null];

      // 78x and 79x Abcasia    
    } else if (countryCode[1] == 8 || countryCode[1] == 9) {
      return ['GE-AB', null];
    } else {
      return ['RU', null];
    }
  }

  return [countryCode, null];
}



MongoClient
  .connect(mongoUrl)
  .then((db) => {

    console.log('Parsing the data');

    data = _(data)
      .map((r) => {
        return r.split(',');
      })
      .map((d) => {
        let date = new Date(year, month, d[5], d[6]);

        let state, subregion

        [state, subregion] = getCountry(d[4]);

        return {
          place: d[0],
          age: d[1],
          type: d[2],
          gender: d[3],
          year: year,
          day: d[5],
          hour: d[6],
          country: state,
          subregion: subregion,
          month: month,
          date: date,
          weekend: (date.getDay() === 6 || date.getDay() === 0),
          count: Number(d[7]),
          nilId: getNilId(d[0])
        };
      })
      .filter((d) => {
        if (d.age == 0 || d.type == 0 || d.gender == 0 || d.gender == null || d.country == 0 || d.country == null || d.place === 'Milano') {
          return false;
        } else {
          return true;
        }
      })
      .value();

    console.log('Inserting data in the db');

    var collection = db.collection(COLLECTION);

    console.log(data.length);
    collection
      .insertMany(data)
      .then(() => {
        console.log('Data inserted');
      })
      .catch((e) => {
        console.log(e);
      });

  })
  .catch((e) => {
    console.log(e);
  })
