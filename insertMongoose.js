/**
 * 建立mongoose模型现在有问题，类型不对
 */
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const mongoose = require('mongoose');

mongoose.connect('mongodb://localhost/2048'); // 连接上数据库
const db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', () => {
  console.log('数据库链接成功');
});
const poi = mongoose.Schema({
  _id: String,
  createTime: {
    type: Date,
    default: Date.now,
  },
  isSync: String,
  tag: {
    id: String,
    lng: Number,
    lat: Number,
    name: String,
    address: String,
    telephone: String,
    type: String,
    areaid: String,
    wgslng: Number,
    wgslat: Number,
    bdlng: Number,
    bdlat: Number,
    updatetime: Date,
    type1: String,
    type2: String,
    type3: String,
    city: String,
    province: String,
  },
//   geom: {
//     type: String,
//     coordinates: [],
//   },
});
const Poi = mongoose.model('newPoi', poi);

function save(records, Model) {
  const match = '_id';
  return new Promise(((resolve, reject) => {
    const bulk = Model.collection.initializeUnorderedBulkOp();
    records.forEach((record) => {
      const query = {};
      query[match] = record[match];
      bulk.find(query).upsert().updateOne(record);
    });
    bulk.execute((err, bulkres) => {
      if (err) return reject(err);
      resolve(bulkres);
    });
  }));
}

const loadCSV = async function () {
  const pathJson = path.resolve(__dirname, './data/poiCopy.csv');
  const stream = fs.createReadStream(pathJson);
  const dates = [];
  csv
    .fromStream(stream, {
      headers: true,
    })
    .on('data', async (data) => {
      const types = data.type.split(';');
      const type1 = types[0];
      const type2 = types[1] ? types[1] : types[0];
      const type3 = types[2] ? types[2] : type2;

      if (dates.length < 1000) {
        const getPoi = {
          _id: data.id,
          isSync: '1',
          tag: {
            id: data.id,
            lng: parseFloat(data.lng),
            lat: parseFloat(data.lat),
            name: data.name,
            address: data.address,
            telephone: data.telephone,
            type: data.type,
            areaid: data.areaid,
            wgslng: parseFloat(data.wgslng),
            wgslat: parseFloat(data.wgslat),
            bdlng: parseFloat(data.bdlng),
            bdlat: parseFloat(data.bdlat),
            updatetime: new Date(data.updatetime),
            type1,
            type2,
            type3,
          },
          geom: {
            type: 'Point',
            coordinates: [
              parseFloat(data.lng),
              parseFloat(data.lat),
            ],
          },
        };
        dates.push(getPoi);
        const vipEntity = new Poi(getPoi);
        vipEntity.save((e) => {
          if (e) {
            console.log(e);
          } else {
            console.log('saved OK!');
          }
        });
      }
    })
    .on('end', () => {
      console.log('结束');
      //   save(dates, Poi).then((bulkRes) => {
      //     console.log('Bulk complete.', bulkRes);
      //     db.close();
      //   }, (err) => {
      //     console.log('Bulk Error:', err);
      //     db.close();
      //   });
    });
};
loadCSV();
