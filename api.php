<?php
header("Access-Control-Allow-Origin: *");
header("Content-Type: application/json; charset=UTF-8");

$host = "localhost";
$db   = "your_db_name";
$user = "your_user";
$pass = "your_password";

try {
    $pdo = new PDO("mysql:host=$host;dbname=$db;charset=utf8", $user, $pass);
} catch (PDOException $e) {
    echo json_encode(["error" => "Connection failed"]);
}

// هنا هنضيف أكواد التسجيل وحفظ الـ CV في الخطوة الجاية
?>