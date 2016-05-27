<?php

include 'connect.inc';

$rows = querya('select :a::integer as a, :b ::integer as b', ['a' => 1, 'b' => 2]);
var_dump($rows);
