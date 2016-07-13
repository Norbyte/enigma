<?php

// Tests that implicit transaction rollback (~pool sweep) is handled correctly

$poolOptions = ['persistent' => true, 'pool_size' => 1];
include 'connect.inc';

$pid0 = get_pg_pid($pool);
query('begin');
// This should roll back the current transaction
$pool->release();

$pool2 = Enigma\create_pool($connectionOptions, $poolOptions);
try {
    $query = new Enigma\Query('select x');
    $resultset = \HH\Asio\join($pool2->query($query));
} catch (Exception $e) {}

// This will throw if there is a transaction in progress
$pid1 = get_pg_pid($pool2);
if ($pid0 != $pid1) echo 'FAIL';
