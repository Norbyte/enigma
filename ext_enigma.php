<?hh // strict

namespace Enigma {

<<__Native>>
function create_pool(array $options) : PoolInterface;


<<__NativeData("PoolInterface")>>
class PoolInterface {
    <<__Native>>
    function query(string $command, array $params) : Awaitable<QueryResult>;
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
    public function fetchArrays() : array;

    <<__Native>>
    public function fetchObjects(string $cls, int $flags) : array;
}

}