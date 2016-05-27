<?php

include 'connect.inc';

$rows = querya('copy test to stdout');
var_dump($rows);
