import axios from 'axios';
import express from 'express';
const app = express();
const router = express.Router(({ mergeParams: true }));
import { escapeHtml, sleep, downloadFile, countFileLines } from './functions.js';
import fs from "fs";
import mysql from "mysql2";
import LineByLineReader from "line-by-line";
import StreamZip from 'node-stream-zip';
import cliProgress from 'cli-progress';
import util from 'util';

const port = 3000;
const connection = await mysql.createConnection({
    host: 'localhost',
    user: 'admin_domains',
    database: 'admin_domains',
    password: 'vHVLHeoSrk'
});

const query = util.promisify(connection.query).bind(connection);

app.use(express.json());
app.use('/', router);

let headers = {
    'Cookie': "XSAE=2d932d061f77e8ca1972a7f0a97ca336; sess_id_=21d4354cb6fe1686b15eeec64a5acb7938e9ea2e"
};

let userAgents = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0',
    'Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
    'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0'
];

let zones = [
    'ru',
    'su',
    'rf'
];

const archivePath = 'archives/';

async function t (zones) {
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

const isUpdate = process.argv.find(el => el.trim() === 'db_update');
if(isUpdate) {
    await t(zones);
}

app.listen(port);