<?php

// Tests that rollback during thread exit works correctly

$poolOptions = ['pool_size' => 2];
include 'connect.inc';

query('begin');
