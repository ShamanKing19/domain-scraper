import express from 'express';
const app = express();
import { 
    escapeHtml, 
    sleep, 
    downloadFile, 
    countFileLines 
} from './functions.js';
import mysql from "mysql2";
import LineByLineReader from "line-by-line";
import StreamZip from 'node-stream-zip';
import cliProgress from 'cli-progress';
import util from 'util';
import 'dotenv/config';

const port = 3000;
const connection = await mysql.createConnection({
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    database: process.env.DB_DATABASE,
    password: process.env.DB_PASSWORD
});

const query = util.promisify(connection.query).bind(connection);

let zones = [
    'ru',
    'su',
    'rf'
];

const archivePath = 'archives/';

async function t(zones) {
    if (!zones.length) return;

    const zone = zones.shift();

    let homeUrl = 'https://statonline.ru/domainlist/file?tld=' + zone;
    await downloadFile(homeUrl, archivePath + zone + '.zip');

    let zip = new StreamZip.async({ file: archivePath + zone + '.zip' });
    const count = await zip.extract(null, archivePath + 'extracted');
    await zip.close();
    console.log('Unpacked: ' + zone + ' archive!\n');

    let filename = archivePath + 'extracted/' + zone + '_domains.txt';
    let lr = new LineByLineReader(filename);
    let linesCount = await countFileLines(filename);

    const bar = new cliProgress.SingleBar({}, cliProgress.Presets.shades_classic);
    bar.start(linesCount, 0);

    let countAdded = 0;
    lr.on('line', function (line) {
        lr.pause();
        countAdded++;
        let domain = line.split(';')[0];

        let arr = [domain, zone];
        let sql = "INSERT IGNORE INTO domains(domain, zone) VALUES(?, ?)";

        (async () => {
            try {
                query(sql, arr).then(r => { bar.increment(); });
            } catch(error) {
                console.log(error);
            }
        })()

        setTimeout(function () {
            lr.resume();
        }, 1);
    });


    lr.on('end', function () {
        t(zones);
        bar.stop();
    })
}

await t(zones);

app.listen(port);