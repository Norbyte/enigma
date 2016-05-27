<?php

include 'connect.inc';

$rows = querya('select * from zero_columns');
var_dump($rows);
