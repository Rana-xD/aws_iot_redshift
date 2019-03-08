const { Client } = require('pg');
const fs = require('fs');
const moment = require('moment');

/**
 * JSONファイルからデータを抽出する
 */
let rawData = fs.readFileSync('data.json');
let data = JSON.parse(rawData);

let redshiftData = {};

const REDSHIFT_HOST = 'daito-iot-redshift.cok2aoomnxsn.ap-northeast-1.redshift.amazonaws.com';
const REDSHIFT_PORT = '5439';
const REDSHIFT_DB = 'dev';
const REDSHIFT_USER = 'awsuser';
const REDSHIFT_PASSWORD = 'cNSj3QNEASc43nw';

/**
 * Redshiftへの接続を作成する
 */
const client = new Client({
    user: REDSHIFT_USER,
    host: REDSHIFT_HOST,
    database: REDSHIFT_DB,
    password: REDSHIFT_PASSWORD,
    port: REDSHIFT_PORT,
});

/**
 * Redshiftに接続する
 */
const redshiftConnect = new Promise((resolve,reject) => {
    client.connect(err => {
        if (err) reject (err);
        resolve({})
    });
});

/**
 * ioデータの横に重要なデータを挿入します。
 */
const insertInfoData = new Promise((resolve,reject)=>{
    redshiftData['sendtime'] = `\'${data.sendTime}\'`;
    redshiftData['id'] = data.id;
    
    redshiftData['timestamp'] = "timestamp" in data? `\'${data.timestamp}\'` : `\'${moment(new Date()).format('YYYY/MM/DD hh:mm:ss')}\'`;
    redshiftData['model_id'] = data.modelId;
    redshiftData['serial_no'] = `\'${data.serialNo}\'`;
    redshiftData['notification_cd'] = data.notificationCd;
    resolve({});
});

/**
 * ioデータ用にデータを操作する
 */
const ioData = new Promise((resolve,reject)=>{
    let propertyNames = Object.keys(data).filter(function (propertyName) {
        return propertyName.includes("No") && propertyName.includes("io");
    });
    propertyNames.forEach((key)=>{
        subKey = key.substr(0,key.indexOf('No'));
        value = data[`${subKey}BitF`] <<  15 | data[`${subKey}BitE`] << 14 | 
                    data[`${subKey}BitD`] << 13 | data[`${subKey}BitC`] << 12 | 
                    data[`${subKey}BitB`] << 11 | data[`${subKey}BitA`] << 10 | 
                    data[`${subKey}Bit9`] << 9  | data[`${subKey}Bit8`] << 8 |
                    data[`${subKey}Bit7`] << 7  | data[`${subKey}Bit6`] << 6 |
                    data[`${subKey}Bit5`] << 5  | data[`${subKey}Bit4`] << 4 |
                    data[`${subKey}Bit3`] << 3  | data[`${subKey}Bit2`] << 2 |
                    data[`${subKey}Bit1`] << 1  | data[`${subKey}Bit0`];
        value = value >= 32768 ? value - 65536 : value; 
        redshiftData[`io${data[key]}`] = value;
    })
    resolve({});
});





/**
 * クエリ文字列を作成する
 */

const query = (data) => {
    let column = Object.keys(data).toString();
    let value = Object.values(data).toString();
    let query = `insert into io_status (${column}) VALUES (${value})`;
    return query;
}

/**
 * Redshiftにデータを挿入する
 */
Promise.all([redshiftConnect,insertInfoData,ioData]).
then(data =>{
    queryData = query(redshiftData);
    console.log(queryData);
    client.query(queryData, (err, res) => {
        if (err) {
            
            console.log(err);
        } else {
            console.log("DONE");
            console.log(res);
        }
    });
    
    
}).catch(err => {
    console.log(err);
})
