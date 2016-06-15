<?php

$poolOptions = ['persistent' => true, 'pool_size' => 1];
include 'connect.inc';

$pid1 = get_pg_pid($pool);

$pool2 = Enigma\create_pool($connectionOptions, $poolOptions);
$pid2 = get_pg_pid($pool2);

$poolNonpersistent = Enigma\create_pool($connectionOptions, []);
$pidNonpersistent = get_pg_pid($poolNonpersistent);

echo ($pid1 === $pid2 && $pidNonpersistent !== $pid1) ? 'OK' : 'FAIL';