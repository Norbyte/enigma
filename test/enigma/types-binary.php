<?php

include 'connect.inc';

$rows = querya("select
    1234::int2 as i2,
    12345678::int4 as i4,
    1234567812345678::int8 as i8,
    2.5::float as f4,
    2.5::double precision as f8,
    false as b1,
    true as b2,
    'aaaa'::text as t,
    ''::char as c,
    'asdasd'::varchar as vc,
    '{}'::json as j,
    '<a />'::xml as x,
    '2015-02-02 03:04:05+05:00'::timestamp as ts,
    '2015-02-02 03:04:05+05:00'::timestamptz as ts2,
    null as n",
    [], Enigma\Query::BINARY);
var_dump($rows);
