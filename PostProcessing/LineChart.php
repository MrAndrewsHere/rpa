<?php

require '../vendor/autoload.php';

use Carbon\Carbon;

/**
 * Class LineChartData
 * Динамика процесса обучения средствами ВКС
 */
class LineChartData
{
    private $db = null;
    private $data = null;
    private $startDate = null;
    private $toDate = null;
    private $withOutput = true;
    private $updated = 0;
    private $inserted = 0;

    public function __construct($db)
    {
        $this->db = $db;

    }

    public function withOutput($output = true)
    {
        $this->withOutput = $output;
        return $this;
    }

    function output($str, $sameLine = false)
    {
        if (!$this->withOutput) {
            return false;
        }
        if ($sameLine) {
            echo chr(27) . "[1A";
        }
        echo __CLASS__.' :: '.$str . PHP_EOL;

    }

    public function from($date = null)
    {
        $this->startDate = $date;
        return $this;
    }

    public function to($date = null)
    {
        $this->toDate = $date;
        return $this;
    }

    public function process()
    {
        $this->startDate = $this->startDate ?? $this->auto_date();
        if ($this->data = $this->get_data()) {
            $start = microtime(true);
            $this->output($this->outputFormat());
            foreach ($this->data as $key => $value) {

                $this->save($value);
                $this->output($this->outputFormat(),true);

            }
            $this->output($this->outputFormat().(round(microtime(true) - $start, 2).' sec'),true);

            return $this->updated+$this->inserted;
        }
        $this->output('I didnt find anything');
        return false;
    }

    function auto_date(){
        $sql = "select max(`dd`) as lastDate from linechart ";
        $res = $this->db->query($sql);
        $this->db->checkError();
        if ($res) {
            $lastDate =  $res[0]['lastDate'];
            return  $lastDate ? Carbon::parse($lastDate)->subDay() : Carbon::yesterday();
        }
    }
    public function truncate()
    {
        $this->db->exec("TRUNCATE TABLE linechart");
        return $this;
    }
    function outputFormat(){
        return  "Updated " . $this->updated . " | "."Inserted " . $this->inserted . " date(s) ";
    }

    function get_data()
    {
        $sql = 'select cnt_pairs40, cnt_sudents40, b.dd from
                (select count(meet_code) as cnt_pairs40,dd from
                    (select meet_code, sum(duration) as sum_time, date as dd
                        from meet_logs_cls where member_id<>org_email and `date` >= ? group by date, meet_code) a where sum_time>2400 group by dd) b
                    left join
                    (select count(member_name) as cnt_sudents40,dd from
                        (select member_name, sum(duration) as sum_time, date as dd
                            from meet_logs_cls where member_id<>org_email and `date` >= ? group by date, member_name) a where sum_time>2400 group by dd) d
                    on b.dd=d.dd where 1 ';


        $result = $this->db->query($sql,[
            'types' => 'ss',
            'values' => [ $this->startDate,$this->startDate]
        ]);
        return $this->db->checkError() ? false : $result;
    }

    function save($data)
    {
        $exists = "select * from linechart where dd = ?";
        if ($res = $this->db->query($exists, ['types' => 's', 'values' => [$data['dd']]])) {
            $update = "update linechart set cnt_pairs40=?, cnt_sudents40=? where dd = ?";
            $this->db->exec($update, [
                'types' => 'sss',
                'values' => array_values($data)
            ]);

            $this->updated++;
            return !$this->db->checkError();

        }

        $sql = "insert into linechart(*fields) values (*values)";
        $this->db->insert($sql, $data);
        $this->inserted++;
        return !$this->db->checkError();

    }
}
