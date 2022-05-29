<?php

function Pagination($countpages)
{
    ?>
    <nav class="pagination-outer" aria-label="Page navigation">
        <ul class="pagination">
            <?
            #НЕ ПУСТОЙ ЧЕКБОКС
            $checkboxcheckedregion = $_GET['search_checkbox_region'];
            $checkboxcheckedmobile = $_GET['search_checkbox_mobile'];
            $checkboxcheckedinn = $_GET['search_checkbox_inn'];
            $checkboxcheckedemail = $_GET['search_checkbox_email'];
            $checkboxcheckedstatus = $_GET['checkbox-search-status'];
            if (!empty($checkboxcheckedregion) || !empty($checkboxcheckedmobile) || !empty($checkboxcheckedinn) || !empty($checkboxcheckedemail) || !empty($checkboxcheckedstatus)) {
                #Выбрали чекбоксы и поиск по тегу
                if (!empty ($_GET['search'])) {
                    $validurl = "&";
                    if ($checkboxcheckedregion != "") {
                        $validurl .= "search_checkbox_region=on&";
                    }
                    if ($checkboxcheckedmobile != "") {
                        $validurl .= "&search_checkbox_mobile=on&";
                    }
                    if ($checkboxcheckedinn != "") {
                        $validurl .= "search_checkbox_inn=on&";
                    }
                    if ($checkboxcheckedemail != "") {
                        $validurl .= "search_checkbox_email=on&";
                    }
                    if ($checkboxcheckedstatus != "") {
                        $validurl .= "checkbox-search-status=$checkboxcheckedstatus&";
                    }
                    $validresults = rtrim($validurl, "&");
                    $validresultswithsearch = $_GET['search'] . $validresults;
                    ?>
                    <li class="page-item">
                        <a href="?search=<? echo $validresultswithsearch ?>" class="page-link" aria-label="Previous">
                            <span aria-hidden="true">« В начало</span>
                        </a>
                    </li>
                    <?
                    if (empty($_GET['page'])) {
                        if ($countpages < 2) {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>">1</a></li>
                            <?
                        } else {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=1">1</a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=2">2</a></li>
                        <?
                        }
                    } else {
                        if ($_GET['page'] == 1) {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } elseif (($_GET['page'] < $countpages) & ($_GET['page'] != 1)) {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } else {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <?
                        }
                    }
                    if ($countpages > 0) {
                        ?>
                        <li class="page-item">
                            <a href="?search=<? echo $validresultswithsearch ?>&page=<? echo $countpages ?>"
                               class="page-link" aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    } else {
                        ?>
                        <li class="page-item">
                            <a href="?search=<? echo $validresultswithsearch ?>" class="page-link" aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    }

                } #Чекбоксы без поиска по тегу
                else {

                    $validurl = "&";
                    if ($checkboxcheckedregion != "") {
                        $validurl .= "search_checkbox_region=on&";
                    }
                    if ($checkboxcheckedmobile != "") {
                        $validurl .= "&search_checkbox_mobile=on&";
                    }
                    if ($checkboxcheckedinn != "") {
                        $validurl .= "search_checkbox_inn=on&";
                    }
                    if ($checkboxcheckedemail != "") {
                        $validurl .= "search_checkbox_email=on&";
                    }
                    if ($checkboxcheckedstatus != "") {
                        $validurl .= "checkbox-search-status=$checkboxcheckedstatus&";
                    }
                    $validresults = rtrim($validurl, "&");
                    $validresultswithsearch = $validresults;
                    ?>
                    <li class="page-item">
                        <a href="?search=<? echo $validresultswithsearch ?>" class="page-link" aria-label="Previous">
                            <span aria-hidden="true">« В начало</span>
                        </a>
                    </li>
                    <?
                    if (empty($_GET['page'])) {
                        if ($countpages < 2) {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>">1</a></li>
                            <?
                        } else {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=1">1</a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=2">2</a></li>
                        <?
                        }
                    } else {
                        if ($_GET['page'] == 1) {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } elseif (($_GET['page'] < $countpages) & ($_GET['page'] != 1)) {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } else {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $validresultswithsearch ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <?
                        }
                    }
                    if ($countpages > 0) {
                        ?>
                        <li class="page-item">
                            <a href="?search=<? echo $validresultswithsearch ?>&page=<? echo $countpages ?>"
                               class="page-link" aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    } else {
                        ?>
                        <li class="page-item">
                            <a href="?search=<? echo $validresultswithsearch ?>" class="page-link" aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    }

                }
            } # ПУСТОЙ ЧЕКБОКС
            else {
                if (!empty ($_GET['search'])) {
                    ?>
                    <li class="page-item">
                        <a href="?search=<? echo $_GET['search'] ?>" class="page-link" aria-label="Previous">
                            <span aria-hidden="true">« В начало</span>
                        </a>
                    </li>
                    <?
                    if (empty($_GET['page'])) {
                        if ($countpages < 2) {
                            ?>
                            <li class="page-item active"><a class="page-link" href="?search=<? echo $_GET['search'] ?>">1</a>
                            </li>
                            <?
                        } else {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $_GET['search'] ?>&page=1">1</a></li>
                            <li class="page-item"><a class="page-link" href="?search=<? echo $_GET['search'] ?>&page=2">2</a>
                            </li>
                        <?
                        }
                    } else {
                        if ($_GET['page'] == 1) {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $_GET['search'] ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $_GET['search'] ?>&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } elseif (($_GET['page'] < $countpages) & ($_GET['page'] != 1)) {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $_GET['search'] ?>&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $_GET['search'] ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $_GET['search'] ?>&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } else {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=<? echo $_GET['search'] ?>&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=<? echo $_GET['search'] ?>&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <?
                        }
                    }
                    if ($countpages > 0) {
                        ?>
                        <li class="page-item">
                            <a href="?search=<? echo $_GET['search'] ?>&page=<? echo $countpages ?>" class="page-link"
                               aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    } else {
                        ?>
                        <li class="page-item">
                            <a href="?search=<? echo $_GET['search'] ?>" class="page-link" aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    }

                } #пустой поиск
                else {

                    ?>
                    <li class="page-item">
                        <a href="?search=&checkbox-search-status=&page=1" class="page-link" aria-label="Previous">
                            <span aria-hidden="true">« В начало</span>
                        </a>
                    </li>
                    <?
                    if (empty($_GET['page'])) {
                        if ($countpages < 2) {
                            ?>
                            <li class="page-item active"><a class="page-link" href="">1</a></li>
                            <?
                        } else {
                            ?>
                            <li class="page-item active"><a class="page-link" href="?search=&checkbox-search-status=&page=1">1</a></li>
                            <li class="page-item"><a class="page-link" href="?search=&checkbox-search-status=&page=2">2</a></li>
                        <?
                        }
                    } else {
                        if ($_GET['page'] == 1) {
                            ?>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=&checkbox-search-status=&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=&checkbox-search-status=&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } elseif (($_GET['page'] < $countpages) & ($_GET['page'] != 1)) {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=&checkbox-search-status=&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=&checkbox-search-status=&page=<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=&checkbox-search-status=&page=<? echo $_GET['page'] + 1 ?>"><? echo $_GET['page'] + 1 ?></a>
                            </li>
                            <?
                        } else {
                            ?>
                            <li class="page-item"><a class="page-link"
                                                     href="?search=&checkbox-search-status=&page=<? echo $_GET['page'] - 1 ?>"><? echo $_GET['page'] - 1 ?></a>
                            </li>
                            <li class="page-item active"><a class="page-link"
                                                            href="?search=&checkbox-search-status=&page==<? echo $_GET['page'] ?>"><? echo $_GET['page'] ?></a>
                            </li>
                            <?
                        }
                    }
                    if ($countpages > 0) {
                        ?>
                        <li class="page-item">
                            <a href="?search=&checkbox-search-status=&page=<? echo $countpages ?>" class="page-link" aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    } else {
                        ?>
                        <li class="page-item">
                            <a href="?search=&checkbox-search-status=" class="page-link" aria-label="Next">
                                <span aria-hidden="true">В конец »</span>
                            </a>
                        </li>
                        <?
                    }
                }
            }
            ?>
        </ul>
    </nav>
    <?
}


?>
