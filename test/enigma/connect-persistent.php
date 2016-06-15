<?php

$connectionOpts = ['host' => '127.0.0.1', 'user' => 'ndo', 'password' => 'ndo', 'dbname' => 'ndo'];
$poolOpts = ['persistent' => true, 'pool_size' => 1];

function get_pg_pid($pool)
{
    $query = new Enigma\Query('select pg_backend_pid() as pid');
    $resultset = \HH\Asio\join($pool->query($query));
    $results = $resultset->fetchArrays();
    return reset($results)['pid'];
}

$pool1 = Enigma\create_pool($connectionOpts, $poolOpts);
$pid1 = get_pg_pid($pool1);

$pool2 = Enigma\create_pool($connectionOpts, $poolOpts);
$pid2 = get_pg_pid($pool2);

$poolNonpersistent = Enigma\create_pool($connectionOpts);
$pidNonpersistent = get_pg_pid($poolNonpersistent);

echo ($pid1 === $pid2 && $pidNonpersistent !== $pid1) ? 'OK' : 'FAIL';