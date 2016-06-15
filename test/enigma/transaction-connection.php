<?php

$poolOptions = ['persistent' => true, 'pool_size' => 3];
include 'connect.inc';

$pid0 = get_pg_pid($pool);

query('begin');
$pid1 = get_pg_pid($pool);
query('select 1');
$pid2 = get_pg_pid($pool);
query('commit');

$pid3 = get_pg_pid($pool);

echo ($pid0 != $pid1 && $pid1 === $pid2 && $pid2 != $pid3) ? 'OK' : 'FAIL';
