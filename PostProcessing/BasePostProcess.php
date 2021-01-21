<?php

interface BaseInterface
{
    function getData();

}

trait BaseMethods
{
    function output($str = '', $sameLine = false)
    {
        if (!$this->withOutput) {
            return false;
        }
        if ($sameLine) {
            echo chr(27) . "[1A";
        }
        echo __CLASS__ . " :: " . $str . PHP_EOL;
    }

    public function truncate($tableName = null)
    {
        if (!$this->table || !$tableName) {
            $this->output("Truncate error: U didnt set table name. Call setTable(tableName : string) or set param truncate(TableName : string) on " . __CLASS__ . " object");
            return $this;
        }
        $table = $this->table ?? $tableName;
        $this->db->exec("TRUNCATE TABLE $table");
        return $this;
    }
}

abstract class BaseClass
{

    use BaseMethods;

    protected $db = null;
    protected $table = null;
    protected $fillable = [];
    protected $data = null;
    protected $withOutput = true;
    protected $counter = 0;
    protected $count = 0;
    protected $limit = 0;
    protected $iterations = -1;

    public function __construct($db,$limit = null,$iterations = null)
    {
        $this->db = $db;
        $this->limit = $limit;
        $this->iterations = $iterations;
    }




    public function process($callback = null)
    {
        $start = microtime(true);
        echo PHP_EOL;
        while ($this->iterations !== 0 && $this->data = $this->getData()) {
            $this->iterations--;

            $this->count += count($this->data);
            call_user_func($callback);

        }
    }
    abstract function getData();

    public function withOutput($output = true)
    {
        $this->withOutput = $output;
        return $this;
    }
    public function setTable($tableName )
    {
        $this->table = $tableName;
        return $this;
    }

    /**
     * @param int|mixed|null $iterations
     */
    public function setIterations(int $iterations = -1)
    {
        $this->iterations = $iterations;
        return $this;
    }

    /**
     * @param int $limit
     */
    public function setLimit(int $limit = null)
    {
        $this->limit = $limit;
        return $this;
    }



    function format(){
        return "Processed  $this->counter of " . $this->count." record(s)";
    }


}
