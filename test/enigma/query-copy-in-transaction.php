<?php

include 'connect.inc';

// Regression test - used to trigger a never-ending wait loop in the pool
query('begin');
try {
    query('copy test from stdin');
} catch (Exception $e) {}

query('rollback');
$rows = querya('select 1 as a');
var_dump($rows);
