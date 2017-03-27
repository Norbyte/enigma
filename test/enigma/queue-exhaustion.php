<?php

include 'connect.inc';

$queries = [];
foreach (range(1, 1000) as $i) {
    $queries[] = $pool->asyncQuery(new Enigma\Query('select 1', []));
}

$wh = \HH\Asio\v($queries);
\HH\Asio\join($wh);
