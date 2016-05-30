<?php

include 'connect.inc';

$rows = querya(<<<SQL
    select '{"key": "val", "bool": false}'::json as json1,
           'false'::json as json2,
           'null'::json as json3,
           12345678::numeric as num,
           '2015-01-01'::date as d,
           '2015-02-02 03:04:05.123123+05:00'::timestamp as ts,
           '2015-02-02 03:04:05.232323+06:00'::timestamp with time zone as tsz
SQL
, [], 0, Enigma\QueryResult::NATIVE | Enigma\QueryResult::NUMERIC_FLOAT
);
var_dump($rows);
