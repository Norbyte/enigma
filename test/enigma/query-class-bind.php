<?php

include 'connect.inc';

class Test
{
    public $a;
}

$rows = queryo('select 1 as a', [], Test::class, Enigma\QueryResult::BIND_TO_PROPERTIES);
var_dump($rows);
