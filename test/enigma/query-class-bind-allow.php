<?php

include 'connect.inc';

class Test
{
    public $b;
}

$rows = queryo('select 1 as a', [], Test::class, Enigma\QueryResult::BIND_TO_PROPERTIES | Enigma\QueryResult::ALLOW_UNDECLARED);
var_dump($rows);
