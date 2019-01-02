<?php

namespace EtlPipeliner;

class AggregateExtractor extends AbstractExtractor
{
    /** @var \ETLPipeliner\AbstractExtractor[] */
    protected $extractors = [];
    
    public function __construct(array $extractors = [])
    {
        $this->extractors = $extractors;
    }

    public function addExtractor(AbstractExtractor $extractor)
    {
        $this->extractors[] = $extractor;
    }

    public function extract($incremental = false): \Generator
    {
        foreach ($this->extractors as $extractor) {
            yield from $extractor->extract();
        }
    }

}
