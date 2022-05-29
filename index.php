<?
# header
require_once __DIR__ . "/search.php";
require_once __DIR__ . "/pagination.php";
require_once __DIR__ . "/db.php";
session_start();
$searchtag = $_GET['search'];
$comment = $_REQUEST['commentary'];
$connection = createConnection()
?>


<link href="/template_styles.css" rel="stylesheet">

<form method="get" id="search" name="search" action="?search=<? echo $searchtag ?>">
    <p><b>Поиск:</b><br>
        <input type="text" name="search" size="100" value="<? echo $searchtag ?>">
        <input type="submit" value="Найти">
    <div class="input-search-area">
        <input type="checkbox" id="search_checkbox_region" name="search_checkbox_region">
        <label for="search_checkbox_region">Регион</label>
        <input type="checkbox" id="search_checkbox_mobile" name="search_checkbox_mobile">
        <label for="search_checkbox_mobile">Телефон</label>
        <input type="checkbox" id="search_checkbox_inn" name="search_checkbox_inn">
        <label for="search_checkbox_mobile">ИНН</label>
        <input type="checkbox" id="search_checkbox_email" name="search_checkbox_email">
        <label for="search_checkbox_email">Email</label>
        <select name="checkbox-search-status">
            <option id="empty" name="empty"></option>
            <option id="nothing" name="nothing">Отсутствует</option>
            <option id="in-process" name="in-process">В процессе</option>
            <option id="finish" name="finish">Завершен</option>
        </select>
    </div>
</form>


<div class="container">
    <div class="row">
        <div class="col-12">
            <div class="card">
                <div class="table-responsive">
                    <table class="table">
                        <thead class="thead-light">
                        <tr>
                            <th scope="col">Адрес</th>
                            <th scope="col">Название</th>
                            <th scope="col">Описание</th>
                            <th scope="col">Регион</th>
                            <th scope="col">Телефон</th>
                            <th scope="col">Email</th>
                            <th scope="col">ИНН</th>
                            <th scope="col">Статус</th>
                            <th scope="col">Комментарий</th>
                            <th scope="col">Сохранить</th>
                        </tr>
                        </thead>

                        <?
                        if (!empty ($_REQUEST['search'])) {
                            search($searchtag, $connection);
                        } ?>
                        <? if (empty($_REQUEST['search'])) {
                            initTable($connection);
                        }
                        getpostOperation($connection)?>

                        <? //end header?>


<?
function initTable($connection)
{
    $checkboxcheckedregion = $_GET['search_checkbox_region'];
    $checkboxcheckedmobile = $_GET['search_checkbox_mobile'];
    $checkboxcheckedinn = $_GET['search_checkbox_inn'];
    $checkboxcheckedemail = $_GET['search_checkbox_email'];
    $checkboxcheckedstatus = $_GET['checkbox-search-status'];
    $page = isset($_GET['page']) ? $_GET['page'] : 1;
    $limit = 10;
    $offset = $limit * ($page - 1);
    $totalpages = countRow(' ', $connection);
    print("ЗАПИСЕЙ ВСЕГО ");
    print_r($totalpages['COUNT(*)']);
    $countpages = round($totalpages['COUNT(*)'] / $limit, 0);
//    print("СТРАНИЦ ВСЕГО ");
//    print_r($countpages);
    if ($_GET['page'] > 1) {
        $realindex = ($_GET['page'] * 10);
    } else {
        $realindex = 0;
    }
    if (empty($checkboxcheckedregion) && empty($checkboxcheckedmobile) && empty($checkboxcheckedinn) && empty($checkboxcheckedemail) && empty($checkboxcheckedstatus)) {
        $results = "SELECT DISTINCT d.real_domain, d.id, d_info.title, d_info.description, d_info.city, d_info.inn, d_info.cms, d_info.status, d_info.comment, c.name, sub_c.name, t.tag, d_info.status, GROUP_CONCAT(d_emails.email), GROUP_CONCAT(d_numbers.number)
            FROM
                domain_info d_info
            INNER JOIN
                domains d on d_info.domain_id = d.id
            LEFT JOIN
                tags t on d_info.tag_id = t.id
            LEFT JOIN 
                subcategory sub_c on t.subcategory_id = sub_c.id
            LEFT JOIN
                category c on sub_c.category_id = c.id
            LEFT JOIN
                domain_emails d_emails on d.id = d_emails.domain_id
            LEFT JOIN
                domain_phones d_numbers on d.id = d_numbers.domain_id
            WHERE
                d.status = 200
            GROUP BY 
                d.real_domain
            LIMIT $limit
            OFFSET $offset;";
    } else {
        $limitoffset = "GROUP BY 
                d.real_domain
            LIMIT $limit
            OFFSET $offset;";
        $results = "SELECT DISTINCT d.real_domain, d.id, d_info.title, d_info.description, d_info.city, d_info.inn, d_info.cms, d_info.status, d_info.comment, c.name, sub_c.name, t.tag, d_info.status, GROUP_CONCAT(d_emails.email), GROUP_CONCAT(d_numbers.number)
            FROM
                domain_info d_info
            INNER JOIN
                domains d on d_info.domain_id = d.id
            LEFT JOIN
                tags t on d_info.tag_id = t.id
            LEFT JOIN 
                subcategory sub_c on t.subcategory_id = sub_c.id
            LEFT JOIN
                category c on sub_c.category_id = c.id
            LEFT JOIN
                domain_emails d_emails on d.id = d_emails.domain_id
            LEFT JOIN
                domain_phones d_numbers on d.id = d_numbers.domain_id";

        $where_str = " WHERE d.status = 200 AND";
        if ($checkboxcheckedregion != "") {
            $where_str .= " d_info.city != '' AND";
        }
        if ($checkboxcheckedmobile != "") {
            $where_str .= " d_numbers.number != '' AND";
        }
        if ($checkboxcheckedinn != "") {
            $where_str .= " d_info.inn != '' AND";
        }
        if ($checkboxcheckedemail != "") {
            $where_str .= " d_emails.email != '' AND";
        }
        if ($checkboxcheckedstatus != "") {
            $where_str .= " d_info.status = '$checkboxcheckedstatus' AND";
        }
        $validresults = rtrim($where_str, "AND");
        $validresults .= $limitoffset;

    }
    $results .= $validresults;
    $recordset = $connection->query($results);
    ?>

    <? while ($row = $recordset->fetch_array()) { ?>
    <? $row['number'] = explode(',', $row['data']); ?>
    <? if ($row['status'] === 'Завершен') { ?>
        <tbody style="background: #91ff74; opacity: 0.8;">
    <?
    } ?>
    <? if ($row['status'] === 'Отсутствует') { ?>
        <tbody style="background: #7a9e9f; opacity: 0.8;">
    <?
    } ?>
    <? if ($row['status'] === 'В процессе') { ?>
        <tbody style="background: #e6ff74; opacity: 0.8;">
    <?
    }
    ?>
    <? $realindex++ ?>
    <tr>
        <td><? echo $row['id'] ?> | <? echo $row['real_domain'] ?></td>
        <td><? echo $row['title'] ?></td>
        <td><? echo $row['description'] ?></td>
        <td><? echo $row['city'] ?></td>
        <td><? echo $row['GROUP_CONCAT(d_numbers.number)'] ?></td>
        <td><? echo $row['GROUP_CONCAT(d_emails.email)'] ?></td>
        <td><? echo $row['inn'] ?></td>
        <form method="post" name="updateTables">
            <td>
                <p class="description">
                    <select name="checkbox_status">
                        <option <?php if ($row['status'] === 'В процессе') {
                            echo("selected");
                        } ?>>В процессе
                        </option>
                        <option <?php if ($row['status'] === 'Завершен') {
                            echo("selected");
                        } ?>>Завершен
                        </option>
                        <option <?php if ($row['status'] === 'Отсутствует') {
                            echo("selected");
                        } ?>>Отсутствует
                        </option>
                    </select>
                </p>
            </td>
            <td>
                <p class="description_input">
                    <textarea name="comment"
                              style="width:auto;height:9.8%;resize: vertical;max-height: 200px;"><? echo $row['comment'] ?></textarea>
                </p>
            </td>
            <td>
                <p class="hideid">
                    <textarea name="search"><? echo $_REQUEST['search'] ?></textarea>
                </p>
                <p class="hideid">
                    <textarea name="cardID"><? echo $row['id'] ?></textarea>
                </p>
                <p class="submit_card">
                    <input type="submit" id="submit_button" value="Сохранить">
                </p>
            </td>
        </form>
    </tr>
<?
}
Pagination($countpages) ?>


    <?


}

?>