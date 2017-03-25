<?php

set_time_limit(0);
echo 'PID: ' . getmypid() . PHP_EOL;

include 'connections.inc';

function pdoQuery($connection, $query, $args, array $opts)
{
    $stmt = array_key_exists('stmt', $opts) ? $opts['stmt'] : null;
    $class = array_key_exists('class', $opts) ? $opts['class'] : null;
    $object = array_key_exists('object', $opts) ? $opts['object'] : false;
    $numbered = array_key_exists('numbered', $opts) ? $opts['numbered'] : false;
    $constructBeforeBinding = array_key_exists('constructBeforeBinding', $opts) ? $opts['constructBeforeBinding'] : false;
    $fetchFlags = $constructBeforeBinding ? PDO::FETCH_PROPS_LATE : 0;

    if (!$stmt) $stmt = $connection->prepare($query);
    if ($args && is_int(reset(array_keys($args)))) {
        foreach ($args as $arg => $value)
            $stmt->bindValue($arg + 1, $value);
    } else {
        foreach ($args as $arg => $value)
            $stmt->bindValue($arg, $value);
    }

    $stmt->execute();
    if ($object) {
        if ($class) {
            return $stmt->fetchAll(PDO::FETCH_CLASS | $fetchFlags, $class);
        } else {
            return $stmt->fetchAll(PDO::FETCH_OBJ | $fetchFlags);
        }
    } elseif ($numbered) {
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    } else {
        return $stmt->fetchAll(PDO::FETCH_NUM);
    }
}

function enigmaQuery($connection, $query, $args, array $opts)
{
    $class = array_key_exists('class', $opts) ? $opts['class'] : '\stdClass';
    $numbered = array_key_exists('numbered', $opts) ? $opts['numbered'] : false;
    $object = array_key_exists('object', $opts) ? $opts['object'] : false;
    $fetchFlags = array_key_exists('fetchFlags', $opts) ? $opts['fetchFlags'] : 0;
    $queryFlags = array_key_exists('queryFlags', $opts) ? $opts['queryFlags'] : 0;
    $async = array_key_exists('async', $opts) ? $opts['async'] : false;
    $constructBeforeBinding = array_key_exists('constructBeforeBinding', $opts) ? $opts['constructBeforeBinding'] : false;
    if ($constructBeforeBinding) $fetchFlags |= Enigma\QueryResult::CONSTRUCT_BEFORE_BINDING;

    $realQuery = new Enigma\Query($query, $args);
    if ($queryFlags & Enigma\Query::CACHE_PLAN) $realQuery->enablePlanCache(true);
    if ($queryFlags & Enigma\Query::BINARY) $realQuery->setBinary(true);
    if ($async) {
        $response = \HH\Asio\join($connection->query($realQuery));
    } else {
        $response = $connection->syncQuery($realQuery);
    }

    if ($object) {
        return $response->fetchObjects($class, $fetchFlags);
    } else {
        if ($numbered) $fetchFlags |= Enigma\QueryResult::NUMBERED;
        return $response->fetchArrays($fetchFlags);
    }
}

function testOnConnection($connection, $text, $query, array $args, array $opts)
{
    $batchSize = array_key_exists('batchSize', $opts) ? $opts['batchSize'] : 1000;
    $prepare = array_key_exists('prepare', $opts) ? $opts['prepare'] : false;
    if ($connection instanceof \PDO) {
        if ($prepare) {
            global $pdo;
            $opts['prepared'] = $connection->prepare($query);
        }

        // Warmup
        for ($i = 0; $i < 3; $i++) {
            pdoQuery($connection, $query, $args, $opts);
        }

        // Benchmark
        $start = microtime(true);
        for ($i = 0; $i < $batchSize; $i++) {
            pdoQuery($connection, $query, $args, $opts);
        }
        $end = microtime(true);
    } else {
        // Warmup
        for ($i = 0; $i < 3; $i++) {
            enigmaQuery($connection, $query, $args, $opts);
        }

        // Benchmark
        $start = microtime(true);
        for ($i = 0; $i < $batchSize; $i++) {
            enigmaQuery($connection, $query, $args, $opts);
        }
        $end = microtime(true);
    }

    return $end - $start;
}

function testQuery($text, $query, array $args = [], array $opts = [])
{
    global $connections;
    $timers = [];
    foreach ($connections as $name => list($base, $connection)) {
        $timers[$name] = testOnConnection($connection, $text, $query, $args, $opts);
    }

    foreach ($connections as $name => list($base, $connection)) {
        $time = $timers[$name] * 1000;
        if ($base !== null) {
            $baseTime = $timers[$base] * 1000;
            $ratio = round((1 - ($time / $baseTime)) * 100, 1);
            $color = $ratio > 0 ? "\x1b[32m" : "\x1b[31m";
            echo sprintf("%-5d (%s%6.1f%%\x1b[0m)  ", $time, $color, $ratio);
        } else {
            echo sprintf("%-5d    ", $time);
        }
    }

    echo $text . PHP_EOL;
}

class TestClass
{
    public $a;
    public $b;
    public $c;
    public $d;
    public $e;
}

foreach ($connections as $name => list($base, $connection)) {
    if ($base === null) {
        echo sprintf('%-9s', $name);
    } else {
        echo sprintf('%-17s', $name);
    }
}
echo 'Test' . PHP_EOL;


testQuery('Short queries',
    'select 1 as a', [],
    ['batchSize' => 10000]);
testQuery('No params/Few cols/Few rows',
    'select 1 as a from generate_series(1, 10)', [],
    ['batchSize' => 10000]);
testQuery('No params/Few cols/Many rows',
    'select 1 as a from generate_series(1, 1000)', [],
    ['batchSize' => 2000]);
testQuery('No params/Many cols/Many rows', 'select 1 as a, 2 as b, 3 as c, 4 as d,
    5 as e, 6 as f, 7 as g, 8 as h, 9 as j,
    10 as k, 11 as l, 12 as m, 13 as n, 14 as o, 15 as p
    from generate_series(1, 500)');
testQuery('Few num params/Few cols/Few rows',
    'select ? ::integer + ? + ? + ? + ? as a from generate_series(1, 10)', [1, 2, 3, 4, 5],
    ['batchSize' => 10000]);
testQuery('Few named params/Few cols/Few rows',
    'select :a::integer + :b + :c + :d + :e as a from generate_series(1, 10)', ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5],
    ['batchSize' => 10000]);
testQuery('Few num params/Few cols/Many rows',
    'select ? ::integer + ? + ? + ? + ? as a from generate_series(1, 1000)', [1, 2, 3, 4, 5],
    ['batchSize' => 2000]);
testQuery('Few num params/Med cols/Many rows',
    'select ? ::integer as a, ? ::integer as b, ? ::integer as c, ? ::integer as d, ? ::integer as e from generate_series(1, 500)', [1, 2, 3, 4, 5],
    ['batchSize' => 2000]);
testQuery('Many num params/Many cols/Many rows',
    'select ? ::integer as a, ? ::integer as b, ? ::integer as c, ? ::integer as d, ? ::integer as e,
    ? ::integer as f, ? ::integer as g, ? ::integer as h, ? ::integer as i, ? ::integer as j ,
    ? ::integer as k, ? ::integer as l, ? ::integer as m, ? ::integer as n, ? ::integer as o ,
    ? ::integer as p, ? ::integer as q, ? ::integer as r, ? ::integer as s, ? ::integer as t
    from generate_series(1, 300)',
    [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20]
);
testQuery('Many named params/Few cols/Few rows',
    'select :a::integer + :b + :c + :d + :e + :f + :g + :h + :i + :j +
            :k + :l + :m + :n + :o + :p + :q + :r + :s + :t as a
     from generate_series(1, 10)',
    ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5, 'f' => 5, 'g' => 5, 'h' => 5, 'i' => 5, 'j' => 5,
     'k' => 1, 'l' => 2, 'm' => 3, 'n' => 4, 'o' => 5, 'p' => 5, 'q' => 5, 'r' => 5, 's' => 5, 't' => 5],
     ['batchSize' => 5000]);
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
testQuery('Integer / binary',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['queryFlags' => Enigma\Query::BINARY]
);
testQuery('Long integer / text',
    'select 1111111 as a, 2222222 as b, 3333333 as c, 4444444 as d, 5555555 as e from generate_series(1, 1000)',
    []
);
testQuery('Long integer / binary',
    'select 1111111 as a, 2222222 as b, 3333333 as c, 4444444 as d, 5555555 as e from generate_series(1, 1000)',
    [],
    ['queryFlags' => Enigma\Query::BINARY]
);
testQuery('Float (textoid) / text',
    'select 11111.1::text as a, 22222.2::text as b, 33333.3::text as c, 44444.4::text as d, 55555.5::text as e from generate_series(1, 200)',
    [],
    ['batchSize' => 2000]
);
testQuery('Float / text',
    'select 11111.1::float4 as a, 22222.2::float4 as b, 33333.3::float4 as c, 44444.4::float4 as d, 55555.5::float4 as e from generate_series(1, 200)',
    []
);
testQuery('Float / binary',
    'select 11111.1::float4 as a, 22222.2::float4 as b, 33333.3::float4 as c, 44444.4::float4 as d, 55555.5::float4 as e from generate_series(1, 200)',
    [],
    ['queryFlags' => Enigma\Query::BINARY]
);
testQuery('Double / text',
    'select 11111.1::float as a, 22222.2::float as b, 33333.3::float as c, 44444.4::float as d, 55555.5::float as e from generate_series(1, 200)',
    []
);
testQuery('Double / binary',
    'select 11111.1::float as a, 22222.2::float as b, 33333.3::float as c, 44444.4::float as d, 55555.5::float as e from generate_series(1, 200)',
    [],
    ['queryFlags' => Enigma\Query::BINARY]
);
testQuery('Bool / text',
    'select true as a, false as b, true as c, false as d, true as e from generate_series(1, 1000)',
    []
);
testQuery('Bool / binary',
    'select true as a, false as b, true as c, false as d, true as e from generate_series(1, 1000)',
    [],
    ['queryFlags' => Enigma\Query::BINARY]
);
testQuery('Short string / text',
    "select 'aa' as a, 'bb' as b, 'cc' as c, 'dd' as d, 'ee' as e from generate_series(1, 1000)",
    []
);
testQuery('Short string / binary',
    "select 'aa' as a, 'bb' as b, 'cc' as c, 'dd' as d, 'ee' as e from generate_series(1, 1000)",
    [],
    ['queryFlags' => Enigma\Query::BINARY]
);
testQuery('Long string / text',
    "select 'aaaaaaaaaaaaaaaaaaaaaaa' as a, 'bbbbbbbbbbbbbbbbbbbbbb' as b,
            'ccccccccccccccccccccccc' as c, 'ddddddddddddddddddddddddddddddd' as d,
            'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as e from generate_series(1, 1000)",
    []
);
testQuery('Long string / binary',
    "select 'aaaaaaaaaaaaaaaaaaaaaaa' as a, 'bbbbbbbbbbbbbbbbbbbbbb' as b,
            'ccccccccccccccccccccccc' as c, 'ddddddddddddddddddddddddddddddd' as d,
            'eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' as e from generate_series(1, 1000)",
    [],
    ['queryFlags' => Enigma\Query::BINARY]
);

testQuery('Med cols/Many rows/AssocArray',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['object' => false]
);
testQuery('Med cols/Many rows/NumberedArray',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['object' => false, 'numbered' => true]
);
testQuery('Med cols/Many rows/StdClass',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['object' => true]
);
testQuery('Med cols/Many rows/UserClass',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['object' => true, 'class' => TestClass::class]
);
testQuery('Med cols/Many rows/UserClass/Bind',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['object' => true, 'class' => TestClass::class, 'fetchFlags' => Enigma\QueryResult::BIND_TO_PROPERTIES]
);
testQuery('Med cols/Many rows/UserClass/BindCBB',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['object' => true, 'class' => TestClass::class, 'fetchFlags' => Enigma\QueryResult::BIND_TO_PROPERTIES, 'constructBeforeBinding' => true]
);
testQuery('Med cols/Many rows/UserClass/BindNoCtor',
    'select 1 as a, 2 as b, 3 as c, 4 as d, 5 as e from generate_series(1, 1000)',
    [],
    ['object' => true, 'class' => TestClass::class, 'fetchFlags' => Enigma\QueryResult::BIND_TO_PROPERTIES | Enigma\QueryResult::DONT_CALL_CTOR]
);

testQuery('Short queries/PlanCache',
    'select 1 as a', [],
    ['queryFlags' => Enigma\Query::CACHE_PLAN]
);
testQuery('Short queries/PlanCache/Prepared Stmt',
    'select 1 as a', [],
    ['queryFlags' => Enigma\Query::CACHE_PLAN, 'prepare' => true]
);
testQuery('4 join queries/PlanCache',
    'select 1 as a
    from    generate_series(1, 1) at
    cross join generate_series(1, 1) bt
    cross join generate_series(1, 1) ct
    cross join generate_series(1, 1) dt',
    [],
    ['queryFlags' => Enigma\Query::CACHE_PLAN]
);
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
    [],
    ['queryFlags' => Enigma\Query::CACHE_PLAN]
);


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

$poolOpts = ['pool_size' => 5];
$enigma = Enigma\create_pool($opts, $poolOpts);

testParallel();
