<?hh // strict

namespace Enigma {

<<__Native>>
function create_pool(array $options, array $poolOptions) : Pool;


<<__NativeData("QueryInterface")>>
class Query {
    <<__Native>>
    function __construct(string $command, array $params = []);

    <<__Native>>
    function enablePlanCache(bool $enabled) : void;

    <<__Native>>
    function setBinary(bool $enabled) : void;
}


<<__NativeData("PoolInterface")>>
class Pool {
    <<__Native>>
    function query(Query $query) : Awaitable<QueryResult>;
}


<<__NativeData("ErrorResult")>>
class ErrorResult extends \Exception {
    <<__Native>>
    public function getMessage() : string;
}


<<__NativeData("QueryResult")>>
class QueryResult {
    <<__Native>>
    public function test() : void;

    <<__Native>>
    public function fetchArrays(int $flags = 0) : array;

    <<__Native>>
    public function fetchObjects(string $cls, int $flags = 0, array $constructorArgs = []) : array;
}

}
