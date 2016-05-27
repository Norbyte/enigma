<?php

include 'connect.inc';

$rows = querya('select ?::integer as a, :b::integer as b', [1, 2]);
var_dump($rows);
