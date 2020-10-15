<?php

namespace EtlPipeliner;

abstract class AbstractEtl
{
    abstract public function extractor(): AbstractExtractor;
    abstract public function loader(): AbstractLoader;

    protected $statistics = [
    ];

    /**
     * @param array $data
     * @return array|Iterator|null
     */
    public function transform(array $data)
    {
        return $data;
    }

    public function getStatistics()
    {
        return $this->statistics;
    }
}
