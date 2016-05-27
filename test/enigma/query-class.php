<?php

include 'connect.inc';

class Test
{
    public $a;
    public $b = 4;
}

$rows = queryo('select 1 as a', [], Test::class);
var_dump($rows);
