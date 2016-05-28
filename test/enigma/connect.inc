<?php

$pool = Enigma\create_pool(['host' => '127.0.0.1', 'user' => 'ndo', 'password' => 'ndo', 'dbname' => 'ndo']);

function query($command, $args = [], $flags = 0)
{
    global $pool;
    $query = new Enigma\Query($command, $args);
    if ($flags & Enigma\Query::CACHE_PLAN) $query->enablePlanCache(true);
    if ($flags & Enigma\Query::BINARY) $query->setBinary(true);
    $response = \HH\Asio\join($pool->query($query));
    return $response;
}

function querya($command, $args = [], $flags = 0)
{
    $results = query($command, $args, $flags);
    return $results->fetchArrays();
}

function queryo($command, $args = [], $cls = null, $fetchFlags = 0, $queryFlags = 0)
{
    $results = query($command, $args, $queryFlags);
    return $results->fetchObjects($cls, $fetchFlags);
}
