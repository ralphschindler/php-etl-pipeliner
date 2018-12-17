<?php

namespace ETLPipeliner\Laravel;

use ETLPipeliner\AbstractLoader;
use Illuminate\Database\Connection;

class DbLoader extends AbstractLoader
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

    public function getUniqueColumns()
    {
        return $this->uniqueColumns;
    }

    public function prepare()
    {
        // build index of existing records
    }

    public function load(array $data)
    {
        // TODO: Implement load() method.
    }

    public function cleanup()
    {
        // find and remove deleted rows
    }
}
