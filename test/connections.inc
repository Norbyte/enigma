<?php

$opts = [
    'host' => '127.0.0.1',
    'user' => 'ndo',
    'password' => 'ndo',
    'dbname' => 'ndo',
    'sslmode' => 'disable'
];
$poolOpts = ['pool_size' => 1];

$connections = [
    'PDO' => [null, new PDO(
                 'pgsql:host=127.0.0.1 user=ndo password=ndo dbname=ndo sslmode=disable',
                 '', '', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION])],
    'Enigma' => ['PDO', Enigma\create_pool($opts, $poolOpts)]
];
