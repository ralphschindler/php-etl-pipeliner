<?php

namespace ETLPipeliner\Laravel;

use ETLPipeliner\AbstractExtractor;
use Illuminate\Database\Connection;

class DbExtractor extends AbstractExtractor
{
    /** @var \Illuminate\Database\Connection */
    protected $connection;
    /** @var \Illuminate\Database\Query\Builder */
    protected $query;
    /** @var array */
    protected $uniqueColumns = ['id'];
    /** @var string */
    protected $updateColumn = 'updated_at';

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
        $this->connection->query();
    }

    public function query()
    {
        return $this->query;
    }

    public function uniqueColumns(array $uniqueColumns)
    {
        $this->uniqueColumns = $uniqueColumns;
    }

    public function getUniqueColumns()
    {
        return $this->uniqueColumns;
    }

    public function createFullHashedIndex()
    {
        // md5 hash of all unique columns concat'd
    }

    public function extract($incremental = false): \Generator
    {
        // TODO: Implement extract() method.
    }
}
