<?php

include 'connect.inc';

$rows = querya("
    select 1 as a,
           9223372036854775807 as aa,
           2.5::float as b,
           2.5 as bb,
           false as c,
           true as d,
           'aaaa' as e,
           null as f");
var_dump($rows);
