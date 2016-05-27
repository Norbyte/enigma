<?php

include 'connect.inc';

$rows = querya('select :a::integer as a, :aa::integer as b, :aaa::integer as c', ['a' => 1, 'aa' => 2, 'aaa' => 3]);
var_dump($rows);
