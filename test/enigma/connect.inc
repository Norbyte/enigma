<?php

include 'functions.inc';

if (!isset($connectionOptions)) {
    $connectionOptions = ['host' => '127.0.0.1', 'user' => 'ndo', 'password' => 'ndo', 'dbname' => 'ndo'];
}

if (!isset($poolOptions)) {
    $poolOptions = [];
}

$pool = Enigma\create_pool($connectionOptions, $poolOptions);
