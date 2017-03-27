<?php

// Tests that explicit transaction rollback is handled correctly

$poolOptions = ['persistent' => true, 'pool_size' => 2];
include 'connect.inc';

query('begin');
$pid0 = get_pg_pid($pool);

$pool2 = Enigma\create_pool($connectionOptions, $poolOptions);
$query = new Enigma\Query('begin');
\HH\Asio\join($pool2->asyncQuery($query));
$pid1 = get_pg_pid($pool2);

query('rollback');
$query = new Enigma\Query('rollback');
\HH\Asio\join($pool2->asyncQuery($query));

$pids = [$pid0 => 0, $pid1 => 0];
for ($i = 0; $i < 30; $i++) {
    $pids[get_pg_pid($pool)]++;
}

if (!$pids[$pid0] || !$pids[$pid1]) echo 'FAIL';
