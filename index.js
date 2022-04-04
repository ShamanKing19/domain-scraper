import parse from 'jquery-html-parser';
import axios from 'axios';
import express from 'express';
const app = express();
const router = express.Router(({ mergeParams: true }));
import phantom from 'phantom';
import { escapeHtml, sleep } from './functions.js';
import ReadText from 'text-from-image';
import Jimp from 'jimp';

const port = 3000;

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

router.get("/start", async function (req, res) {
    /*const response = await axios.get('https://i.imgur.com/hzsNhsA.png',  { responseType: 'arraybuffer' })
    const image = await Jimp.read(response.data);
    image.color([{apply:'darken', params: [30]}]).write('as.png');

    image.getBuffer(Jimp.MIME_PNG, (err, buffer) => {
        ReadText(buffer).then(text => {
            console.log(text);
        }).catch(err => {
            console.log(err);
        })
    });*/

    /*try {
        let data;
        for (let i = 1; i < 50000; i++) {
            let homeUrl = 'https://statonline.ru/domains?page=' + i +
                '&rows_per_page=200&order=ASC&sort_field=domain_name_idn&registered=REGISTERED&tld=ru' +
                '&l_9c1b65077e36b0b9d90075edbb7d868c=1';

            console.log(homeUrl);
            headers["User-Agent"] = userAgents[Math.floor(Math.random() * userAgents.length)];
            const response = await axios.get(homeUrl, {
                headers: headers,
                method: 'GET',
                maxRedirects: 50,
            });

            data = response.data.toString();
            const html = parse(data);

            if(i === 1) {
                break;
            }

            console.log(response.data);
        }

        res.json(data);
    } catch(err) {
        return res.status(403).json({
            'status': false,
            'error': err.message,
        });
    }*/

    const instance = await phantom.create();
    const page = await instance.createPage();
    //https://phantomjs.org/api/webpage/handler/on-resource-requested.html
    await page.on('onResourceRequested', function(requestData) {
        //console.info('Requesting', requestData.url);
    });

    let result = [];
    for (let i = 1; i < 50000; i++) {
        let homeUrl = 'https://statonline.ru/domains?page=' + i +
            '&rows_per_page=200&order=ASC&sort_field=domain_name_idn&registered=REGISTERED&tld=ru' +
            '&l_9c1b65077e36b0b9d90075edbb7d868c=1';

        let status = await page.open(homeUrl);
        let content = await page.property('content');
        let $ = parse(content)

        let tbody = $('table.rustat_table tbody tr');

        if (tbody.length) {
            tbody.each(function () {
                let td = $(this).find('td');
                let domainTd = td.eq(2);
                if (domainTd) {
                    let domainHref = domainTd.find('a');
                    if (domainHref) {
                        let href = domainHref.attr('href');
                        let urlParams = new URLSearchParams(href)
                        if (urlParams.has('domain')) {
                            let domainName = urlParams.get('domain');
                            if (domainName) {
                                result.push(domainName);
                            }
                        }

                    }
                }
            });
        }

        await sleep(2);

        if(i >= 5) {
            break;
        }
    }

    res.json(result);

    await instance.exit();
});

app.listen(port);