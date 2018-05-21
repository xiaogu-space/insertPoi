/**
 * 通过数据插入，但是数组长度不能无限长
 */
const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const url = 'mongodb://localhost:27017/';
let allCount = 0;
let number = 0;

const loadCSV = async function () {
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
	  number += 1;
      console.log(`${data.name}  ${number}`);
      //   if (dates.length < 1200) {
      dates.push({
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
      });
    //   }
    })
    .on('end', async () => {
      MongoClient.connect(url, (err, db) => {
        if (err) throw err;
        const dbo = db.db('2048');
        const connect = dbo.collection('newPoi');
        insertData(dates, connect, db);
      });
    });
};
const insertData = function (datas, connect, db) {
  if (datas.length > 0) { // 有数据就插入
    const insertDatas = datas.length > 1000 ? datas.slice(0, 1000) : datas;
    const lastDatas = datas.length > 1000 ? datas.slice(1000) : [];
    connect.insertMany(insertDatas, (err, res) => {
      if (err) throw err;
      allCount += res.insertedCount;
      console.log(`插入的文档数量为: ${allCount}`);
      insertData(lastDatas, connect, db);
    });
  } else {
    db.close();
    process.exit();
  }
};
loadCSV();
