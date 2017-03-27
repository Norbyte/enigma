<?php

function query($command, $args = [], $flags = 0)
{
    global $pool;
    $query = new Enigma\Query($command, $args);
    if ($flags & Enigma\Query::CACHE_PLAN) $query->enablePlanCache(true);
    if ($flags & Enigma\Query::BINARY) $query->setBinary(true);
    $response = \HH\Asio\join($pool->asyncQuery($query));
    return $response;
}

function syncQuery($command, $args = [], $flags = 0)
{
    global $pool;
    $query = new Enigma\Query($command, $args);
    if ($flags & Enigma\Query::CACHE_PLAN) $query->enablePlanCache(true);
    if ($flags & Enigma\Query::BINARY) $query->setBinary(true);
    return $pool->syncQuery($query);
}

function querya($command, $args = [], $flags = 0, $fetchFlags = 0)
{
    $results = query($command, $args, $flags);
    return $results->fetchArrays($fetchFlags);
}

function queryo($command, $args = [], $cls = null, $fetchFlags = 0, $queryFlags = 0)
{
    $results = query($command, $args, $queryFlags);
    return $results->fetchObjects($cls, $fetchFlags);
}

function get_pg_pid($pool)
{
    $query = new Enigma\Query('select pg_backend_pid() as pid');
    $resultset = $pool->syncQuery($query);
    $results = $resultset->fetchArrays();
    return reset($results)['pid'];
}
