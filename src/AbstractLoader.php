<?php

namespace EtlPipeliner;

abstract class AbstractLoader
{
    abstract public function load(array $data);

    public function prepare()
    {
    }

    public function cleanup()
    {
    }

    public function getStatistics()
    {
        return [];
    }
}
