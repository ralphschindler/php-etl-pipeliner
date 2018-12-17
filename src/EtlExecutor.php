<?php

namespace ETLPipeliner;

class EtlExecutor
{
    public function execute(AbstractEtl $etl, $incremental = false)
    {
        $extractor = $etl->extractor();
        $loader = $etl->loader();

        $loader->prepare();

        foreach ($extractor->extract($incremental) as $data) {
            $loader->load($etl->transform($data));
        }

        $loader->cleanup();
    }
}