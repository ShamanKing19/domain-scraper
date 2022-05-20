import {userAgents} from './functions.js';
import axios from 'axios';
import mysql from 'mysql2';
import 'dotenv/config';
import https from 'http'
import fs from "fs";

async function makeRequestAsync(domain, index) {
    let url = "http://" + domain;

    await axios.get(url, {
        timeout: 5000,
        key: fs.readFileSync('agent2-key.pem'),
        cert: fs.readFileSync('agent2-cert.pem'),
        userAgents: userAgents(),
        maxRedirects: 3,
        httpsAgent: new https.Agent({
            rejectUnauthorized: false
        })
    }).then(function (response) {
        console.log("№" + index + " " + response.config['url'] + " " + response.status);

    }).catch(function (err) {
        console.log("№" + index + " " + url + " " + err.message);
    });
}

function parseDomains(previewPortion, tempPortion) {
    const connection = mysql.createConnection({
        host: "localhost",
        user: "stydent",
        database: "lemon5",
        password: "stydent"
    });

    let sql_query = "SELECT domain FROM domains WHERE ID BETWEEN "+ previewPortion +" AND "+ tempPortion +"";
    connection.query(sql_query, function (error, result) {

        if (!error) {
            for (let item of result) {
                makeRequestAsync(item['domain'], tempPortion++);
            }

        } else {
            throw new Error(error);
        }
    });


    connection.end(function (err) {
        if (err) {
            console.log(err);
        } else {
            console.log("Соединение закрыто!");
        }
    });
}


let tempPortion = 0;
let previewPortion=90;
function echoPortion(){
    const portion = 90;
    previewPortion = tempPortion-portion+90;
    tempPortion = tempPortion+portion;
    parseDomains(previewPortion,tempPortion);
}

setInterval(function () {
    echoPortion();
}, 5010);






