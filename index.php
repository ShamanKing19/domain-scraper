<?
# header
require_once __DIR__ . "/search.php";
require_once __DIR__ . "/pagination.php";
require_once __DIR__ . "/db.php";
session_start();
$searchtag = $_GET['search'];
$comment = $_REQUEST['commentary'];
$connection = createConnection();
getpostOperation($connection);
ini_set('display_errors', 0);
ini_set('display_startup_errors', 0);
error_reporting(0);
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
                        }?>

<? //end header?>
