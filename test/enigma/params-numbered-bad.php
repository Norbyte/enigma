<?php

include 'connect.inc';

$rows = querya('select ?::integer as a, ? ::integer as b', [1]);
var_dump($rows);
