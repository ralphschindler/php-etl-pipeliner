<?php

namespace ETLPipeliner;

abstract class AbstractLoader
{
    abstract public function load(array $data);

    public function prepare()
    {
    }

    public function cleanup()
    {
    }
}
