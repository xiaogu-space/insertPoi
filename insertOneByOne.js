const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const url = 'mongodb://localhost:27017/';
let number = 0;

MongoClient.connect(url, (err, db) => {
  if (err) throw err;
  const dbo = db.db('2048');
  const connect = dbo.collection('newPoi');
  loadCSV(connect, db);
});

const loadCSV = async function (connect, db) {
  const pathJson = path.resolve(__dirname, './data/poi_amap_0510.csv');
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

      const insertData = {
        _id: data.id,
        createTime: new Date(),
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
          city: `${data.areaid.slice(0, 4)}00`,
          province: `${data.areaid.slice(0, 2)}0000`,
        },
        geom: {
          type: 'Point',
          coordinates: [
            parseFloat(data.lng),
            parseFloat(data.lat),
          ],
        },
      };
      connect.insertOne(insertData, (err, res) => {
        if (err) throw err;
        number += 1;
        console.log(`文档插入成功,数量  ${number}`);
      });
    })
    .on('end', async () => {
      db.close();
    });
};
