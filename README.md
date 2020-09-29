# PHP ETL Pipeliner

Define ETL pipelines to extract, transform, and load data from one source to another.

# Installation

    composer require ralphschindler/etl-pipeliner

# Usage

To build an ETL pipeline you need the ETL, an Extractor, a Loader and an Executor.

## ETL object

Implement a class extending `\EtlPipeliner\AbstractEtl`. This package ships with an extractor and a loader for use within Laravel application. 

```php
class MyEtlObject extends \EtlPipeline\AbstractEtl
{
    public function extractor(): \EtlPipeliner\AbstractExtractor
    {
        return new \EtlPipeliner\Laravel\DbExtractor(app('db')->connection());
    }

    public function transform(array $data)
    {
        return $data;
    }

    public function loader(): \EtlPipeliner\AbstractLoader
    {
            return new \EtlPipeliner\Laravel\DbLoader(app('db')->connection());
    }
}
```

## Execute the ETL

```php
$executor = new \EtlPipeliner\EtlExecutor();

$executor->execute(new MyEtlObject());
```

# Database support

The Laravel extractor and loader currently support:

- [x] Mysql
- [x] SQL Server
- [x] Postgres
