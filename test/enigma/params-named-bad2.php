<?php

include 'connect.inc';

querya('select :a::integer as a, :b ::integer as b', ['a' => 1, 'c' => 2]);
