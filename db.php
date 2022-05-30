<?php
// composer require vlucas/phpdotenv --prefer-dist
require __DIR__ . '/vendor/autoload.php';

$dotenv = Dotenv\Dotenv::createImmutable(__DIR__);
try {
    $dotenv->load();
    $dotenv->required(['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_DATABASE'])->notEmpty();
} catch ( Exception $e )  {
    echo $e->getMessage();
}
function createConnection(){
    $hostname = $_ENV ['DB_HOST'];
    $username=$_ENV ['DB_USER'];
    $password=$_ENV ['DB_PASSWORD'];
    $database=$_ENV ['DB_DATABASE'];
    $connection = new mysqli($hostname, $username, $password, $database);
    $connection->set_charset("utf8mb4");
    return $connection;
}
