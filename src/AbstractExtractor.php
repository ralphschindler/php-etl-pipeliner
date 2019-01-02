<?php

namespace EtlPipeliner;

abstract class AbstractExtractor
{
    abstract public function extract($incremental = false): \Generator;
}
