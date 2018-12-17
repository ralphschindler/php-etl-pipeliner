<?php

namespace ETLPipeliner;

abstract class AbstractExtractor
{
    abstract public function extract($incremental = false): \Generator;

    public function getIndex(): array
    {
        return [];
    }
}
