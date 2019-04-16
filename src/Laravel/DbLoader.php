<?php

namespace EtlPipeliner\Laravel;

use EtlPipeliner\AbstractLoader;
use Illuminate\Database\Connection;
use Illuminate\Database\MySqlConnection;

class DbLoader extends AbstractLoader
{
    /** @var \Illuminate\Database\Connection */
    protected $connection;
    /** @var string */
    protected $table = '';
    /** @var array */
    protected $columns;
    /** @var array */
    protected $columnsForInsertOnly = [];
    /** @var array */
    protected $columnsForUpdateOnly = [];
    /** @var array */
    protected $uniqueColumns = ['id'];
    /** @var string */
    protected $updateColumn = 'updated_at';
    /** @var array */
    protected $rowHashIndex = [];
    /** @var \PDOStatement */
    protected $insertStatement = null;
    /** @var \PDOStatement */
    protected $updateStatement = null;
    /** @var bool */
    protected $inTransaction = false;
    /** @var int */
    protected $iteration = 0;
    /** @var bool */
    protected $ignoreDuplicates = true;
    /** @var callable|null */
    protected $onInsertDataAppender;
    /** @var callable|null */
    protected $onUpdateDataAppender;
    /** @var array */
    protected $statistics = [
        'insert_count' => 0,
        'update_count' => 0,
        'delete_count' => 0
    ];

    public function __construct(Connection $connection)
    {
        $this->connection = $connection;
    }

    public function table($table)
    {
        $this->table = $table;
        return $this;
    }

    public function columns(array $columns, array $insertOnlyColumns = [], array $updateOnlyColumns = [])
    {
        $this->columns = $columns;
        $this->columnsForInsertOnly = $insertOnlyColumns;
        $this->columnsForUpdateOnly = $updateOnlyColumns;
        return $this;
    }

    public function uniqueColumns($uniqueColumns)
    {
        $this->uniqueColumns = $uniqueColumns;
        return $this;
    }

    public function getUniqueColumns()
    {
        return $this->uniqueColumns;
    }

    public function onInsertDataAppender(callable $onInsertDataAppender)
    {
        $this->onInsertDataAppender = $onInsertDataAppender;
    }
    
    public function onUpdateDataAppender(callable $onUpdateDataAppender)
    {
        $this->onUpdateDataAppender = $onUpdateDataAppender;
    }
    
    public function prepare()
    {
        $this->createIndex();
    }
    
    public function load(array $data)
    {
        $this->iteration++;
        if ($this->iteration % 1000 === 1) {
            $this->connection->beginTransaction();
        } elseif ($this->iteration % 1000 === 0) {
            $this->connection->commit();
        }

        $hash = $data['_hash'] ?? null;
        unset($data['_hash']);

        if (isset($this->rowHashIndex[$hash])) {
            $this->updateRow($data);
        } else {
            $this->insertRow($data);
        }

        $this->rowHashIndex[$hash] = true;
    }

    public function cleanup()
    {
        if ($this->connection->transactionLevel() > 0) {
            $this->connection->commit();
        }
    }

    public function getStatistics()
    {
        return $this->statistics;
    }

    protected function createIndex()
    {
        $grammar = $this->connection->getQueryGrammar();
        $table = $grammar->wrapTable($this->table);

        if ($this->connection instanceof MySqlConnection) {
            $sql = 'SELECT MD5(CONCAT_WS("|", ' . implode(', ', $this->getUniqueColumns()) . ")) as hash FROM {$table}";
        } else {
            throw new \RuntimeException('Only MySQL currently supported as loader type');
        }

        foreach ($this->connection->select($sql) as $row) {
            $this->rowHashIndex[$row->hash] = false;
        }
    }

    protected function updateRow(array $data)
    {
        if (!$this->updateStatement) {
            $this->prepareUpdateStatement();
        }

        try {
            if (is_callable($this->onUpdateDataAppender)) {
                $data = call_user_func($this->onUpdateDataAppender, $data);
                if (!is_array($data)) {
                    throw new \RuntimeException('onUpdateDataAppender() should return an array of $data to insert');
                }
            }
            $this->updateStatement->execute($this->createBindingData($data, array_merge($this->columns, $this->columnsForUpdateOnly)));
            $this->statistics['update_count'] += $this->updateStatement->rowCount();
        } catch (\Exception $e) {
            throw $e;
        }
    }

    protected function insertRow(array $data)
    {
        if (!$this->insertStatement) {
            $this->prepareInsertStatement();
        }

        try {
            if (is_callable($this->onInsertDataAppender)) {
                $data = call_user_func($this->onInsertDataAppender, $data);
                if (!is_array($data)) {
                    throw new \RuntimeException('onUpdateDataAppender() should return an array of $data to insert');
                }
            }
            $this->insertStatement->execute($this->createBindingData($data, array_merge($this->columns, $this->columnsForInsertOnly)));
            $this->statistics['insert_count'] += $this->insertStatement->rowCount();
        } catch (\Exception $e) {
            throw $e;
        }
    }

    protected function createBindingData($data, $columns)
    {
        $newData = [];
        foreach ($columns as $column) {
            $newData[':' . $column] = $data[$column] ?? null;
        }
        return $newData;
    }

    protected function prepareInsertStatement()
    {
        $columns = collect($this->columns);

        if ($this->columnsForInsertOnly) {
            $columns = $columns->merge($this->columnsForInsertOnly);
        }

        $grammar = $this->connection->getQueryGrammar();

        $table = $grammar->wrapTable($this->table);

        $this->insertStatement = $this->connection->getPdo()->prepare(
            "INSERT INTO {$table} ("
            . $columns->map(function ($column) use ($grammar) { return $grammar->wrap($column); })->implode(', ')
            . ') VALUES ('
            . $columns->map(function ($column) { return ':' . $column; })->implode(', ')
            . ')'
        );
    }

    protected function prepareUpdateStatement()
    {
        $uniqueColumns = $this->getUniqueColumns();
        $sqlSets = $sqlWheres = [];

        $grammar = $this->connection->getQueryGrammar();

        $columns = collect($this->columns);

        if ($this->columnsForUpdateOnly) {
            $columns = $columns->merge($this->columnsForUpdateOnly);
        }

        foreach ($columns as $column) {
            $partialSql = $grammar->wrap($column) . " = :{$column}";
            if (!in_array($column, $uniqueColumns)) {
                $sqlSets[] = $partialSql;
            } else {
                $sqlWheres[] = $partialSql;
            }
        }

        if (!$sqlSets || !$sqlWheres) {
            throw new \RuntimeException('A proper update statement could not be produced, either set columns or where columns are missing.');
        }

        $this->updateStatement = $this->connection->getPdo()->prepare(
            'UPDATE ' . $grammar->wrapTable($this->table)
            . ' SET ' . implode(', ', $sqlSets)
            . ' WHERE ' . implode(' AND ', $sqlWheres)
        );
    }
}
