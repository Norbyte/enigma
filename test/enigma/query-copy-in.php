<?php

include 'connect.inc';

$rows = querya('copy test from stdin');
var_dump($rows);
