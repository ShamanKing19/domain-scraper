import Axios from "axios";
import fs from "fs";

export function escapeHtml(text) {
    let map = {
        '&': '&amp;',
        '<': '&lt;',
        '>': '&gt;',
        '"': '&quot;',
        "'": '&#039;'
    };

    return text.replace(/[&<>"']/g, function(m) { return map[m]; });
}

export function userAgents() {
    return [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:95.0) Gecko/20100101 Firefox/95.0',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.110 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; rv:91.0) Gecko/20100101 Firefox/91.0',
        'Mozilla/5.0 (X11; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.71 Safari/537.36',
        'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:95.0) Gecko/20100101 Firefox/95.0'
    ];
}

export function sleep(ms) {
    return new Promise((resolve) => {
        setTimeout(resolve, ms);
    });
}

export async function downloadFile(fileUrl, outputLocationPath) {
    const writer = fs.createWriteStream(outputLocationPath);

    return Axios({
        method: 'get',
        url: fileUrl,
        responseType: 'stream',
    }).then(response => {
        return new Promise((resolve, reject) => {
            response.data.pipe(writer);
            let error = null;
            
            writer.on('error', err => {
                error = err;
                writer.close();
                reject(err);
            });
            
            writer.on('close', () => {
                if (!error) {
                    resolve(true);
                }
            });
        });
    });
}

function contains(str, text) {
    return str.indexOf(text) >= 0;
}

export async function countFileLines(filePath) {
    return new Promise((resolve, reject) => {
        let lineCount = 0;
        fs.createReadStream(filePath)
            .on("data", (buffer) => {
                let idx = -1;
                lineCount--;
                do {
                    idx = buffer.indexOf(10, idx+1);
                    lineCount++;
                } while (idx !== -1);
            }).on("end", () => {
            resolve(lineCount);
        }).on("error", reject);
    });
}

export function getCms(html) {
    if(contains(html, '<link href="/bitrix/js/main')) {
        return 'Bitrix';
    } else if(contains(html, '/wp-content/themes/')) {
        return 'Wordpress';
    } else if(contains(html, '<meta name="modxru"')) {
        return 'ModX';
    } else if(contains(html, '<script type="text/javascript" src="/netcat')) {
        return 'Netcat';
    } else if(contains(html, '<script src="/phpshop')) {
        return 'PhpShop';
    } else if(contains(html, '<script type="text/x-magento-init')) {
        return 'Magento';
    } else if(contains(html, '/wa-data/public')) {
        return 'Shop-Script';
    } else if(contains(html, 'catalog/view/theme')) {
        return 'OpenCart';
    } else if(contains(html, 'data-drupal-')) {
        return 'Drupal';
    } else if(contains(html, '<meta name="generator" content="Joomla')) {
        return 'Joomla';
    } else if(contains(html, 'var dle_admin')) {
        return 'DataLife Engine';
    }

    return '';
}