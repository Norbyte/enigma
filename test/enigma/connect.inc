<?php

$pool = Enigma\create_pool(['host' => '127.0.0.1', 'user' => 'ndo', 'password' => 'ndo', 'dbname' => 'ndo']);

function query($command, $args = [])
{
    global $pool;
    $query = new Enigma\Query($command, $args);
    $response = \HH\Asio\join($pool->query($query));
    return $response;
}

function querya($command, $args = [])
{
    $results = query($command, $args);
    return $results->fetchArrays();
}

function queryo($command, $args = [], $cls = null, $flags = 0)
{
    $results = query($command, $args);
    return $results->fetchObjects($cls, $flags);
}
