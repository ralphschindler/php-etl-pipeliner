<?php

namespace EtlPipeliner;

class EtlExecutor
{
    public function execute(AbstractEtl $etl, $incremental = false)
    {
        $extractor = $etl->extractor();
        $loader = $etl->loader();

        $loader->prepare();

        foreach ($extractor->extract($incremental) as $data) {
            $transformed = $etl->transform($data);
            
            if ($transformed instanceof \Iterator) {
                foreach ($transformed as $transformedData) {
                    $loader->load($transformedData);
                }
            } elseif ($transformed !== null) {
                $loader->load($transformed);
            }
        }

        $loader->cleanup();
    }
}