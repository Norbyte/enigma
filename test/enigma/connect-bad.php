<?php

$pool = Enigma\create_pool(['host' => '127.0.0.1', 'user' => 'bad', 'password' => 'bad', 'dbname' => 'bad']);
$query = new Enigma\Query('select 1', []);
\HH\Asio\join($pool->query($query));
