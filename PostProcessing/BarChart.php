<?php

require '../vendor/autoload.php';

use Carbon\Carbon;

/**
 * Class BarChart
 * Посещаемость (%)
 */
class BarChart
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
        echo __CLASS__ . ' :: ' . $str . PHP_EOL;

    }

    public function from($date = null)
    {
        if ($date) {
            $this->startDate = Carbon::parse($date);
        }

        return $this;
    }

    function auto_date(){
        $sql = "select max(`day`) as lastDate from barchart ";
        $res = $this->db->query($sql);
        $this->db->checkError();
        if ($res) {
            $lastDate =  $res[0]['lastDate'];
            return  $lastDate ? Carbon::parse($lastDate)->subDay() : Carbon::yesterday();
        }
    }
    public function to($date = null)
    {
        if ($date) {

            $this->toDate = Carbon::parse($date);
        }

        return $this;
    }

    public function process()
    {
        $startTime = microtime(true);
        $this->startDate = $this->startDate ?? $this->auto_date();
        $this->toDate = $this->toDate ?? Carbon::now();

        $days = [];
        $start = $this->startDate->copy();
        while ($start->lessThan($this->toDate->format('Y-m-d'))) {
            $days[] = "'" . $start->format('Y-m-d') . "'";
            $start->addDay();
        }
        if (count($days) > 0) {
            $this->output('');
            foreach ($days as $day) {

                $result = $this->get_data($day);

                if ($result) {
                    $this->save($result) ? $this->output($this->outputFormat(), true) : null;;
                }

            }
            $this->output($this->outputFormat() . (round(microtime(true) - $startTime, 2) . ' sec'), true);
            return $this->updated + $this->inserted;
        }
        $this->output('I didnt find anything');
        return false;

    }


    public function truncate()
    {
        $this->db->exec("TRUNCATE TABLE barchart");
        return $this;
    }

    function outputFormat()
    {
        return "Updated " . $this->updated . " | " . "Inserted " . $this->inserted . " date(s) ";
    }

    function get_data($day)
    {
        $sql = 'select DATE_FORMAT(' . $day . ', "%Y-%m-%d")  as day, avgs, cnt, ROUND(((avgs*100)/cnt),1) as p from
                          (select avg(cnt_sudents40) as avgs from
                            (select count(member_name) as cnt_sudents40,dd from
                                (select member_name, sum(duration) as sum_time, date as dd from
                                    meet_logs_cls where member_id<>org_email group by date, member_name) a
                            where sum_time>2400 and dd>=' . $day . ' and dd<=' . $day . ' group by dd) d
                          ) av, (select groupRank,count(*) as cnt from contingent_etalon  where groupRank = "default" group by groupRank limit 1 ) ct';


        $result = $this->db->query($sql);
        $this->db->checkError();
        if($result && count($result) >0)
        {
            return $result[0];
        }
        return false;
    }

    function save($data)
    {

        $exists = "select * from barchart where `day` = ?";
        if ($res = $this->db->query($exists, ['types' => 's', 'values' => [$data['day']]])) {
            $update = "update barchart set avgs=?, cnt=?, p=? where `day` = ?";
            $this->db->exec($update, [
                'types' => 'siss',
                'values' => [
                    $data['avgs'],
                    $data['cnt'],
                    $data['p'],
                    $data['day']
                ]
            ]);
            if ($this->db->checkError()) {
                return false;
            }
            $this->updated++;
            return true;

        }
        $sql = "insert into barchart(*fields) values (*values)";
        $this->db->insert($sql, $data);

        if ($this->db->checkError()) {
            return false;
        }
        $this->inserted++;
        return true;

    }
}
