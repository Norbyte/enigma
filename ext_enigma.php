<?hh // strict

<<__Native>>
function enigma_create_pool(array $options) : EnigmaQueue;


<<__NativeData("EnigmaPoolInterface")>>
class EnigmaPoolInterface {
    <<__Native>>
    function query(string $command, array $params) : Awaitable<EnigmaQueryResult>;
}


<<__NativeData("EnigmaErrorResult")>>
class EnigmaErrorResult extends Exception {
    <<__Native>>
    public function getMessage() : string;
}


<<__NativeData("EnigmaQueryResult")>>
class EnigmaQueryResult {
    <<__Native>>
    public function test() : void;

    <<__Native>>
    public function fetchArrays() : array;

    <<__Native>>
    public function fetchObjects(string $cls, int $flags) : array;
}
