<?php

namespace EtlPipeliner;

abstract class AbstractEtl
{
    abstract public function extractor(): AbstractExtractor;
    abstract public function loader(): AbstractLoader;

    /**
     * @param array $data
     * @return array|Iterator|null
     */
    public function transform(array $data)
    {
        return $data;
    }
}
