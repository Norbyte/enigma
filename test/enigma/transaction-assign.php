<?php

// Tests that a connection with an open transaction won't get assigned
// to other handles on the same pool

$poolOptions = ['persistent' => true, 'pool_size' => 2];
include 'connect.inc';

query('begin');
$pid1 = get_pg_pid($pool);

$pool2 = Enigma\create_pool($connectionOptions, $poolOptions);
for ($i = 0; $i < 10; $i++) {
    $pid2 = get_pg_pid($pool2);
    if ($pid2 == $pid1) echo 'FAIL';
}

