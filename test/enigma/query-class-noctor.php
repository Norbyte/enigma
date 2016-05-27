<?php

include 'connect.inc';

class Test
{
    public $a;
    public $b = 4;

    public function __construct()
    {
        $this->b = 3;
    }
}

$rows = queryo('select 1 as a', [], Test::class, Enigma\QueryResult::DONT_CALL_CTOR);
var_dump($rows);
