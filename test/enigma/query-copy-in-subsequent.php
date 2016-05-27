<?php

include 'connect.inc';

try {
    querya('copy test from stdin');
} catch (Exception $e) {}

$rows = querya('select 1 as a');
var_dump($rows);
