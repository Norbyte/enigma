<?php

include 'connect.inc';

$rows = querya('select :a::integer as a, :a::integer as b, :a::integer as c', ['a' => 1]);
var_dump($rows);
