<?php
function countRow($searchtag, $connection)
{
    $checkboxcheckedregion = $_GET['search_checkbox_region'];
    $checkboxcheckedmobile = $_GET['search_checkbox_mobile'];
    $checkboxcheckedinn = $_GET['search_checkbox_inn'];
    $checkboxcheckedemail = $_GET['search_checkbox_email'];
    $checkboxcheckedstatus = $_GET['checkbox-search-status'];
    if (!empty($checkboxcheckedregion) || !empty($checkboxcheckedmobile) || !empty($checkboxcheckedinn) || !empty($checkboxcheckedemail) || !empty($checkboxcheckedstatus)) {
        if (!empty ($_GET['search'])) {
            $results = "
                SELECT COUNT(*) FROM domains 
                JOIN domain_info ON domain_info.domain_id = domains.id
                JOIN tags ON  tags.id = domain_info.tag_id        
                
                ";
            $dopjoin = " ";
            $where_str = " WHERE tags.tag LIKE '%$searchtag%' AND ";
            if ($checkboxcheckedregion != "") {
                $where_str .= " domain_info.city != '' AND";
            }
            if ($checkboxcheckedmobile != "") {
                $dopjoinmobile = "JOIN domain_phones on domains.id = domain_phones.domain_id ";
                $where_str .= " domain_phones.number != '' AND";
            }
            if ($checkboxcheckedinn != "") {
                $where_str .= " domain_info.inn != '' AND";
            }
            if ($checkboxcheckedemail != "") {
                $dopjoinemail = "JOIN domain_emails on domains.id = domain_emails.domain_id ";
                $where_str .= " domain_emails.email != '' AND";
            }
            if ($checkboxcheckedstatus != "") {
                $where_str .= " domain_info.status = '$checkboxcheckedstatus' AND";
            }
            $dopjoin .= $dopjoinmobile . $dopjoinemail;
            $validjoin = rtrim($dopjoin, "JOIN");
            $validresults = rtrim($where_str, "AND");
            $results .= $validjoin . $validresults;
        }
        if (empty ($_GET['search'])) {
            $results = "
                SELECT COUNT(*)
                FROM domain_info 
                ";
            $where_str = "WHERE ";
            $dopjoin = " ";
            if ($checkboxcheckedregion != "") {
                $where_str .= " domain_info.city != '' AND";
            }
            if ($checkboxcheckedmobile != "") {
                $dopjoinmobile = "JOIN domain_phones on domains.id = domain_phones.domain_id ";
                $where_str .= " domain_phones.number != '' AND";
            }
            if ($checkboxcheckedinn != "") {
                $where_str .= " domain_info.inn != '' AND";
            }
            if ($checkboxcheckedemail != "") {
                $dopjoinemail = "JOIN domain_emails on domains.id = domain_emails.domain_id ";
                $where_str .= " domain_emails.email != '' AND";
            }
            if ($checkboxcheckedstatus != "") {
                $where_str .= " domain_info.status = '$checkboxcheckedstatus' AND";
            }
            if (!empty($checkboxcheckedmobile) || !empty($checkboxcheckedemail)) {
                $dopjoin .= "JOIN domains ON domain_info.domain_id = domains.id " . $dopjoinmobile . $dopjoinemail;
            }
            if (empty($checkboxcheckedmobile) && empty($checkboxcheckedemail)) {
                $dopjoin .= $dopjoinmobile . $dopjoinemail;
            }
            $validjoin = rtrim($dopjoin, "JOIN");
            $validresults = rtrim($where_str, "AND");
            $results .= $validjoin . $validresults;
        }
    } else {
        if (!empty ($_GET['search'])) {
            $results = "
                SELECT COUNT(*) FROM domains 
                JOIN domain_info ON domain_info.domain_id = domains.id
                JOIN tags ON  tags.id = domain_info.tag_id
                WHERE tags.tag LIKE '%$searchtag%'
                ";

        }
        if (empty ($_GET['search'])) {
            $results = "
                SELECT COUNT(*)
                FROM domain_info 
                ";
        }
    }
    $recordset = $connection->query($results);
    return $recordset->fetch_array();
}
function getpostOperation($connection){
    $tempsearchtag = $_SESSION['search'];
    if (isset($_POST['checkbox_status'])) {
        $id = $_POST['cardID'];
        $checkboxStatus = $_POST['checkbox_status'];
        $cardResult = "UPDATE domain_info
                    SET status = '$checkboxStatus'
                    WHERE domain_info.domain_id = '$id';";
        $connection->queryExecute($cardResult);
    }

    if ($_POST['comment'] && $_POST['cardID']) {
        $id = $_POST['cardID'];
        $comment = $_POST['comment'];
        $cardResult = "UPDATE domain_info
                    SET comment = '$comment'
                    WHERE domain_info.domain_id = '$id';";
        $connection->queryExecute($cardResult);
    }
}
?>

<?

?>

<?
function search($searchtag, $connection){
    $page = isset($_GET['page']) ? $_GET['page'] : 1;
    $limit = 10;
    $offset = $limit * ($page - 1);
    $totalpages = countRow($searchtag, $connection);
    $correcttotalpages = $totalpages['COUNT(*)'];
    $countpages = round($correcttotalpages / $limit, 0);
    print("ЗАПИСЕЙ ВСЕГО ");
    print_r($totalpages['COUNT(*)']);
//    print("КОЛИЧЕСТВО СТРАНИЦ ");
//    print_r($countpages);
//    print_r($searchtag);
    if ($_GET['page'] > 1) {
        $realindex = ($_GET['page'] * 10);
    } else {
        $realindex = 0;
    }
    if (!empty($searchtag)){
    $checkboxcheckedregion = $_GET['search_checkbox_region'];
    $checkboxcheckedmobile = $_GET['search_checkbox_mobile'];
    $checkboxcheckedinn = $_GET['search_checkbox_inn'];
    $checkboxcheckedemail = $_GET['search_checkbox_email'];
    $checkboxcheckedstatus = $_GET['checkbox-search-status'];

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
                    t.tag LIKE '%$searchtag%' AND d.status = 200 
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
                domain_phones d_numbers on d.id = d_numbers.domain_id
            ";

    $where_str = " WHERE t.tag LIKE '%$searchtag%' AND d.status = 200 AND";
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

    <? while ($row = $recordset->fetch()) { ?>
    <? $realindex++ ?>
    <? $row['number'] = explode(',', $row['data']); ?>
    <? if ($row['status'] === 'Завершен'){ ?>
    <tbody style="background: #91ff74; opacity: 0.8;">
    <?
    } ?>
    <? if ($row['status'] === 'Отсутствует'){ ?>
    <tbody style="background: #7a9e9f; opacity: 0.8;">
    <?
    } ?>
    <? if ($row['status'] === 'В процессе'){ ?>
    <tbody style="background: #e6ff74; opacity: 0.8;">
    <?
    } ?>
    <tr>
        <td><? echo $row['id'] ?> | <? echo $row['real_domain'] ?></td>
        <td><? echo $row['title'] ?></td>
        <td><? echo $row['description'] ?></td>
        <td><? echo $row['city'] ?></td>
        <td><? echo $row['GROUP_CONCAT(d_numbers.number)'] ?></td>
        <td><? echo $row['GROUP_CONCAT(d_emails.email)'] ?></td>
        <td><? echo $row['inn'] ?></td>
        <form method="post" name="updateTables">
        <td><p class="description">
            <p class="hideid">
                <textarea name="search"><? echo $searchtag ?></textarea>
            </p>
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
                <textarea name="cardID"><? echo $realindex ?></textarea>
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

} ?>



