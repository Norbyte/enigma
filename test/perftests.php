<?php

$pdo = new PDO(
    'pgsql:host=127.0.0.1 user=ndo password=ndo dbname=ndo sslmode=disable',
    '', '', [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

$opts = [
    'host' => '127.0.0.1',
    'user' => 'ndo',
    'password' => 'ndo',
    'dbname' => 'ndo',
    'sslmode' => 'disable'
];
$poolOpts = ['pool_size' => 5];
$enigma = Enigma\create_pool($opts, $poolOpts);

function pdoQuery($query, $args, $object, $class, $stmt)
{
    global $pdo;
    if (!$stmt) $stmt = $pdo->prepare($query);
    if ($args && is_int(reset(array_keys($args)))) {
        foreach ($args as $arg => $value)
            $stmt->bindValue($arg + 1, $value);
    } else {
        foreach ($args as $arg => $value)
            $stmt->bindValue($arg, $value);
    }

    $stmt->execute();
    if ($object) {
        return $stmt->fetchAll(PDO::FETCH_CLASS, $class);
    } else {
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }
}

function enigmaQuery($query, $args, $object, $class, $flags, $queryFlags)
{
    global $enigma;
    $realQuery = new Enigma\Query($query, $args);
    if ($queryFlags & Enigma\Query::CACHE_PLAN) $realQuery->enablePlanCache(true);
    if ($queryFlags & Enigma\Query::BINARY) $realQuery->setBinary(true);
    $response = \HH\Asio\join($enigma->query($realQuery));

    if ($object) {
        return $response->fetchObjects($class ?? '\stdClass', $flags);
    } else {
        return $response->fetchArrays();
    }
}

function testQuery($text, $query, $args = [], $object = false, $class = null, $flags = 0, $queryFlags = 0, $prepare = false)
{
    if ($prepare) {
        global $pdo;
        $prepared = $pdo->prepare($query);
    } else {
        $prepared = null;
    }

    // Warmup
    for ($i = 0; $i < 3; $i++) {
        pdoQuery($query, $args, $object, $class, $prepared);
    }

    // Benchmark
    $pdoStart = microtime(true);
    for ($i = 0; $i < 1000; $i++) {
        pdoQuery($query, $args, $object, $class, $prepared);
    }
    $pdoEnd = microtime(true);

    // Warmup
    for ($i = 0; $i < 3; $i++) {
        enigmaQuery($query, $args, $object, $class, $flags, $queryFlags);
    }

    // Benchmark
    $enigmaStart = microtime(true);
    for ($i = 0; $i < 1000; $i++) {
        enigmaQuery($query, $args, $object, $class, $flags, $queryFlags);
    }
    $enigmaEnd = microtime(true);

    $pdoTime = ($pdoEnd - $pdoStart) * 1000;
    $enigmaTime = ($enigmaEnd - $enigmaStart) * 1000;
    $ratio = round((1 - ($enigmaTime / $pdoTime)) * 100, 1);
    $color = $ratio > 0 ? "\x1b[32m" : "\x1b[31m";
    echo sprintf("%-6d   %-6d   %s%-5.1f%% \x1b[0m%s" . PHP_EOL,
        $pdoTime, $enigmaTime, $color, $ratio, $text);
}

class TestClass
{
    public $a;
    public $b;
    public $c;
    public $d;
    public $e;
}

echo "PDO      Enigma   Test" . PHP_EOL;
testQuery('Short queries', 'select 1 as a');
testQuery('No params/Few cols/Few rows', 'select 1 as a from generate_series(1, 10)');
testQuery('No params/Few cols/Many rows', 'select 1 as a from generate_series(1, 1000)');
testQuery('No params/Many cols/Many rows', 'select 1 as a, 2 as b, 3 as c, 4 as d,
    5 as e, 6 as f, 7 as g, 8 as h, 9 as j,
    10 as k, 11 as l, 12 as m, 13 as n, 14 as o, 15 as p
    from generate_series(1, 500)');
testQuery('Few num params/Few cols/Few rows', 'select ? ::integer + ? + ? + ? + ? as a from generate_series(1, 10)', [1, 2, 3, 4, 5]);
testQuery('Few named params/Few cols/Few rows', 'select :a::integer + :b + :c + :d + :e as a from generate_series(1, 10)', ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]);
testQuery('Few num params/Few cols/Many rows', 'select ? ::integer + ? + ? + ? + ? as a from generate_series(1, 1000)', [1, 2, 3, 4, 5]);
testQuery('Few num params/Med cols/Many rows', 'select ? ::integer as a, ? ::integer as b, ? ::integer as c, ? ::integer as d, ? ::integer as e from generate_series(1, 500)', [1, 2, 3, 4, 5]);
testQuery('Many num params/Many cols/Many rows',
    'select ? ::integer as a, ? ::integer as b, ? ::integer as c, ? ::integer as d, ? ::integer as e,
    ? ::integer as f, ? ::integer as g, ? ::integer as h, ? ::integer as i, ? ::integer as j ,
    ? ::integer as k, ? ::integer as l, ? ::integer as m, ? ::integer as n, ? ::integer as o ,
    ? ::integer as p, ? ::integer as q, ? ::integer as r, ? ::integer as s, ? ::integer as t
    from generate_series(1, 300)',
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
);
testQuery('Huge num params/No cols/No rows',
    'with t as (select ' . str_repeat('? ::integer, ', 1000) . '1) select 1',
    array_fill(0, 1000, 1)
);
testQuery('Huge num params/Few cols/Few rows',
    'with t as (select ' . str_repeat('? ::integer, ', 1000) . '1) select 1, 2, 3, 4 from generate_series(1, 10)',
    array_fill(0, 1000, 1)
);

testQuery('Integer / text',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    []
);
testQuery('Long integer / text',
    'select 1111111 as a, 2222222 as b, 3333333 as c, 4444444 as d, 5555555 as e from generate_series(1, 1000)',
    []
);
testQuery('Float / text',
    'select 11111.1::float as a, 22222.2::float as b, 33333.3::float as c, 44444.4::float as d, 55555.5::float as e from generate_series(1, 1000)',
    []
);
testQuery('Bool / text',
    'select true as a, false as b, true as c, false as d, true as e from generate_series(1, 1000)',
    []
);
testQuery('Short string / text',
    "select 'aa' as a, 'bb' as b, 'cc' as c, 'dd' as d, 'ee' as e from generate_series(1, 1000)",
    []
);
testQuery('Long string / text',
    "select 'aaaaaaaaaaaaaaaaaaaaaaa' as a, 'bbbbbbbbbbbbbbbbbbbbbb' as b,
            'ccccccccccccccccccccccc' as c, 'ddddddddddddddddddddddddddddddd' as d,
            'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as e from generate_series(1, 1000)",
    []
);

testQuery('Integer / binary',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [], false, null, 0, Enigma\Query::BINARY
);
testQuery('Long integer / binary',
    'select 1111111 as a, 2222222 as b, 3333333 as c, 4444444 as d, 5555555 as e from generate_series(1, 1000)',
    [], false, null, 0, Enigma\Query::BINARY
);
testQuery('Float / binary',
    'select 11111.1::float as a, 22222.2::float as b, 33333.3::float as c, 44444.4::float as d, 55555.5::float as e from generate_series(1, 1000)',
    [], false, null, 0, Enigma\Query::BINARY
);
testQuery('Bool / binary',
    'select true as a, false as b, true as c, false as d, true as e from generate_series(1, 1000)',
    [], false, null, 0, Enigma\Query::BINARY
);
testQuery('Short string / binary',
    "select 'aa' as a, 'bb' as b, 'cc' as c, 'dd' as d, 'ee' as e from generate_series(1, 1000)",
    [], false, null, 0, Enigma\Query::BINARY
);
testQuery('Long string / binary',
    "select 'aaaaaaaaaaaaaaaaaaaaaaa' as a, 'bbbbbbbbbbbbbbbbbbbbbb' as b,
            'ccccccccccccccccccccccc' as c, 'ddddddddddddddddddddddddddddddd' as d,
            'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as e from generate_series(1, 1000)",
    [], false, null, 0, Enigma\Query::BINARY
);

testQuery('Med cols/Many rows/StdClass',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [], true
);
testQuery('Med cols/Many rows/UserClass',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [], true, TestClass::class
);
testQuery('Med cols/Many rows/UserClass/Bind',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [], true, TestClass::class, ENIG_BIND_TO_PROPERTIES
);
testQuery('Med cols/Many rows/UserClass/BindNoCtor',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [], true, TestClass::class, ENIG_BIND_TO_PROPERTIES | ENIG_DONT_CALL_CTOR
);

testQuery('Short queries/PlanCache', 'select 1 as a', [], false, null, 0, Enigma\Query::CACHE_PLAN);
testQuery('Short queries/PlanCache/Prepared Stmt', 'select 1 as a', [], false, null, 0, Enigma\Query::CACHE_PLAN, true);
testQuery('4 join queries/PlanCache',
    'select 1 as a
    from    generate_series(1, 1) at
    cross join generate_series(1, 1) bt
    cross join generate_series(1, 1) ct
    cross join generate_series(1, 1) dt',
    [], false, null, 0, Enigma\Query::CACHE_PLAN);
testQuery('16 join queries/PlanCache',
    'select 1 as a
    from    generate_series(1, 1) at
    cross join generate_series(1, 1) bt
    cross join generate_series(1, 1) ct
    cross join generate_series(1, 1) dt
    cross join generate_series(1, 1) et
    cross join generate_series(1, 1) ft
    cross join generate_series(1, 1) gt
    cross join generate_series(1, 1) ht
    cross join generate_series(1, 1) it
    cross join generate_series(1, 1) jt
    cross join generate_series(1, 1) kt
    cross join generate_series(1, 1) lt
    cross join generate_series(1, 1) mt
    cross join generate_series(1, 1) nt
    cross join generate_series(1, 1) ot
    cross join generate_series(1, 1) pt',
    [], false, null, 0, Enigma\Query::CACHE_PLAN);


function testParallel()
{
    global $enigma;
    echo 'Testing parallel launch ... ' . PHP_EOL;

    $times = [];
    for ($conns = 1; $conns < 10; $conns++) {
        $start = microtime(true);
        for ($i = 0; $i < 5000 / $conns; $i++) {
            $queries = [];
            foreach (range(1, $conns) as $_) {
                $query = new Enigma\Query('select 1 as a, 2 as b, 3 as c from generate_series(1, 100) t');
                $queries[] = $enigma->query($query);
            }

            $response = \HH\Asio\join(\HH\Asio\v($queries));
        }

        $end = microtime(true);
        $time = ($end - $start) * 1000;
        $times[$conns] = $time;
        if ($conns > 1) {
            $ratio = round((1 - ($time / $times[1])) * 100, 1);
            echo $conns . ': ' . round($time) . ' ms' . '(' . $ratio . '%)' . PHP_EOL;
        } else {
            echo $conns . ': ' . round($time) . ' ms' . PHP_EOL;
        }
    }
}

testParallel();