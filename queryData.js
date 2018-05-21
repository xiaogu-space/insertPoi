const MongoClient = require('mongodb').MongoClient;
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');

const url = 'mongodb://localhost:27017/';
// MongoClient.connect(url, function (err, db) {
//     if (err) throw err;
//     const dbo = db.db("2048");
//     // dbo.collection("dataSet_poi_amap").find().limit(10).toArray(function (err, result) { // 返回集合中所有数据
//     //     if (err) throw err;
//     //     console.log(result);
//     //     db.close();
//     // });
//     const dataMap = new Map();
//     dbo.collection("dataSet_poi_amap").distinct('tag.type1', {}, (function (err, docs1) {
//         for (const type1 of docs1) {
//             if (!dataMap.has(type1)) {
//                 dataMap.set(type1, type1);
//                 console.log(type1);
//             }
//         }
//         dbo.collection("dataSet_poi_amap").distinct('tag.type2', {}, (function (err, docs2) {
//             for (const type2 of docs2) {
//                 if (!dataMap.has(type2)) {
//                     dataMap.set(type2, type2);
//                     console.log(type2);
//                 }
//             }
//             dbo.collection("dataSet_poi_amap").distinct('tag.type3', {}, (function (err, docs3) {
//                 for (const type3 of docs3) {
//                     if (!dataMap.has(type3)) {
//                         dataMap.set(type3, type3);
//                         console.log(type3);
//                     }
//                 }
//             }));
//         }));
//     }));
// });
const loadCSV = async function (fileName) {
  const pathJson = path.resolve(__dirname, './data/poi_amap_0510.csv');
  const stream = fs.createReadStream(pathJson);
  const dataMap = new Map();
  csv
    .fromStream(stream, {
      headers: true,
    })
    .on('data', async (data) => {
      const types = data.type.split(';');
      for (const type of types) {
        const typ = type.split('|');
        for (const ty of typ) {
          if (!dataMap.has(ty)) {
            dataMap.set(ty, ty);
            console.log(ty);
          }
        }
      }
    })
    .on('end', async () => {
      process.exit();
    });
};
// loadCSV();
