<?php

namespace ETLPipeliner;

abstract class AbstractEtl
{
    // build index to process

    // look for deleted items in index (full left and full right)

    abstract public function extractor(): AbstractExtractor;
    abstract public function loader(): AbstractLoader;

    public function transform(array $data)
    {
        return $data;
    }
}
