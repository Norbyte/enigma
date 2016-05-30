<?php

include 'connect.inc';

$rows = querya('select 1 as a, 2 as b', [], 0, Enigma\QueryResult::NUMBERED);
var_dump($rows);
