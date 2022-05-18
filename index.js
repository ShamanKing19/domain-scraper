import { userAgents } from './functions.js';
import axios from 'axios';
import mysql from 'mysql2';
import 'dotenv/config';
import https from 'http'

async function makeRequestAsync(domain, index) {
    let url = "http://" + domain;

    await axios.get(url, {
        timeout: 5000,
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

async function parseDomains() {
    const connection = mysql.createConnection({
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        database: process.env.DB_DATABASE,
        password: process.env.DB_PASSWORD
    });

    let i = 0;
    let sql_query = "SELECT domain FROM domains LIMIT 1000";
    connection.query(sql_query, function (error, result) {
        
        if (!error) {
            //version 3
            for (let item of result) {
                makeRequestAsync(item['domain'], ++i);
            }
            //version2
            /*
            let i = 0;
            result.forEach(async (item) => {
                await makeRequestAsync(item['domain'], ++i);
            });
            */
    
            //version 1
            /*
            result.forEach(function (current_value, index, array) {
                makeRequestAsync(current_value['domain'], index); // TODO: здесь await
            });
            */
        }


        else {
            throw new Error(error);
        }
    });



    connection.end(function (err) {
        if (err) {
            console.log(err);
        }
        else {
            console.log("Соединение закрыто!");
        }
    });
}

parseDomains();
