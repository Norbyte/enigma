<?php

include 'connect.inc';

$rows = querya(<<<'SQL'
    select null::integer[] as n,
           array[]::integer[] as i1,
           array[1, 2, 3]::integer[] as i2,
           array[-1234, -123131132]::integer[] as i3,
           array[11, null, 22]::integer[] as i_n,
           array[12.34, -111222.333]::float[] as f,
           array[true, false]::bool[] as b,
           array[]::text[] as t1,
           array['asd']::text[] as t2,
           array['asd', 'bsd']::text[] as t3,
           array['asd', '', 'bsd']::text[] as t4,
           array['asd', 'bsd', '']::text[] as t5,
           array['', null, 'NULL', '', 'a', null] as t6,
           array['"{}''', '\', '123'] as t7,
           array[12345678, 1234]::numeric[] as num,
           array['2015-01-01', '2015-06-06']::date[] as d
SQL
, [], 0, Enigma\QueryResult::NATIVE | Enigma\QueryResult::NUMERIC_FLOAT
);
var_dump($rows);
