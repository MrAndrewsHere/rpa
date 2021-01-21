<?php
require '../vendor/autoload.php';

use Carbon\Carbon;

/**
 * Class Attendance Счиатем посещаемость
 */
class Attendance
{

    private $db = null;
    private $rawData = null;
    private $PreparedData = null;
    private $pairs = null;
    private $fromDate=null;
    private $toDate = 0;
    private $limit = 0;
    private $NumberOfIterations = -1;
    private $withOutput = true;
    private $counter = 0;
    private $count = 0;

    public function __construct($db, $limit,$iterations)
    {
        $this->db = $db;
        $this->limit = $limit;
        $iterations ?  $this->NumberOfIterations = $iterations : null;

    }
    public function process()
    {
        $start = microtime(true);
        $this->fromDate = $this->fromDate ?? $this->auto_date();

        $this->get_secondary_data();

        $this->output();
        $this->output("Processed  $this->counter of " . $this->count." record(s)",true);
        while ($this->NumberOfIterations !== 0 && $this->get_raw_data()) {


            $this->NumberOfIterations--;
            $this->count += count($this->rawData);

            foreach ($this->rawData as $raw) {
                $temp = $raw;
                $temp['pair'] = $this->get_pair($raw);
                $temp['group'] = $this->get_group_name($raw);
                $temp['lecturer'] = $this->get_lecture($raw);
                if ($this->saveRaw($temp)) {
                    $this->counter++;
                    $this->output("Processed  $this->counter of " . $this->count." record(s)",true);


                }

            }


        }
        $this->output("Processed  $this->counter of " . $this->count." record(s) in ".(round(microtime(true) - $start, 2).' sec'),true);

        return $this->counter;


    }
    function auto_date(){
        $sql = "select max(`date`) as lastDate from meet_logs_cls ";
        $res = $this->db->query($sql);
        $this->db->checkError();
        if ($res) {
            $lastDate =  $res[0]['lastDate'];
            return  $lastDate ? Carbon::parse($lastDate)->subDay()->format('Y-m-d') : Carbon::yesterday()->format('Y-m-d');
        }
    }
    public function withOutput($output = true)
    {
        $this->withOutput = $output;
        return $this;
    }
    function output($str='', $sameLine = false)
    {
        if (!$this->withOutput) {
            return false;
        }
        if ($sameLine) {
            echo chr(27) . "[1A";
        }
        echo __CLASS__.' :: '.$str . PHP_EOL;
    }
    public function reprocess(){
        $this->rollback();
        $this->process();
    }



    function get_secondary_data()
    {
        $this->pairs = $this->db->query('select * from academic_pair');

    }
    public function from($date = null)
    {
        $this->fromDate = $date;
        return $this;
    }

    public function to($date = null)
    {
        $this->toDate = $date;
        return $this;
    }

    function get_raw_data()
    {

        $sql = "select
                                     m.id,
                                     m.date,
                                     m.times,
                                     m.times as time_end,
                                     SUBTIME(m.times,SEC_TO_TIME(m.duration)) as start_tim,
                                     SUBTIME(m.times,SEC_TO_TIME(m.duration)) as time_start,
                                     m.meet_code,
                                     m.member_id,
                                     m.org_email,
                                     m. product_type,
                                     (m.duration DIV 60) as duration,
                                     m.member_name,
                                     m.id_meet,
                                     m.processed,
                                     m.process_state,
                                     m.pair,
                                     a.id as from_academic_pairs,
                                     m.group
                                     from meet_logs_cls m
                                      left join academic_pair a on (
                                            (SUBTIME(m.times,SEC_TO_TIME(m.duration))>=a.time_start and m.times<=a.time_end)
                                            or (SUBTIME(m.times,SEC_TO_TIME(m.duration))>=SUBTIME(a.time_start,SEC_TO_TIME(900)) and m.times<=ADDTIME(a.time_end,SEC_TO_TIME(900)))
                                            or (m.times<a.time_end and m.times>a.time_start and SUBTIME(m.times,SEC_TO_TIME(m.duration)) >= a.time_start)
                                            or ((m.duration DIV 60) >= 90
                                                and (m.duration DIV 60) <= 148
                                                and  hour(SUBTIME(m.times,SEC_TO_TIME(m.duration))) = hour(a.time_start)
                                                or (hour(SUBTIME(m.times,SEC_TO_TIME(m.duration)))+1) = hour(a.time_start)
                                                and  hour(m.times)= hour(a.time_end))
                                            or ((m.duration DIV 60) >=20
                                                and (m.duration DIV 60) < 90
                                                and (hour(SUBTIME(m.times,SEC_TO_TIME(m.duration)))+1) = hour(a.time_end)))
                                     where m.processed = 0
                                      and SUBTIME(m.times,SEC_TO_TIME(m.duration))>='08:20:00'
                                      and m.times>='09:05:00'
                                      and SUBTIME(m.times,SEC_TO_TIME(m.duration))<='19:20:00'
                                      and m.times<='19:40:00'
                                      and (m.duration DIV 60) > 5
                                   
                                  ";
        $sql .= $this->fromDate ? (" and m.`date` >=" . "'" . $this->fromDate . "'") : null;
        $sql .= $this->toDate ? (" and m.`date` <=" . "'" . $this->toDate . "'") : null;
        $this->limit > 0 ? $sql .= " limit $this->limit" : null;

        $result = $this->db->query($sql);


        if ($this->db->isError()) {
            echo($this->db->getLastError());

        }


        if ($result) {

            $this->rawData = $result;
            return true;
        }
        return false;

    }


    function get_pair($raw)
    {

        if ($raw['from_academic_pairs']) return $raw['from_academic_pairs'];

        $pair = null;
        $start_time_rounded = Carbon::parse($raw['time_start'])->roundMinute(5);
        $end_time_rounded = Carbon::parse($raw['time_end'])->roundMinute(5);
        $duration = $raw['duration'];

        $pair = array_filter($this->pairs, function ($v) use ($start_time_rounded, $end_time_rounded, $duration) {
            return ($duration <= 100 && (($end_time_rounded->diffInMinutes(Carbon::parse($v['time_end'])) <= 20)));
        });
        if (!$pair) {
            $pair = array_filter($this->pairs, function ($v) use ($start_time_rounded, $end_time_rounded, $duration) {
                return ($duration <= 30 && (($start_time_rounded->diffInMinutes(Carbon::parse($v['time_start'])) <= 20)));
            });
        }


        if (!$pair) {
            $pair = array_filter($this->pairs, function ($v) use ($start_time_rounded, $end_time_rounded, $duration) {
                return ($duration <= 100 && (($start_time_rounded->diffInMinutes(Carbon::parse($v['time_start'])) <= 20)));
            });
        }

        if (!$pair) {
            $pair = array_filter($this->pairs, function ($v) use ($start_time_rounded, $end_time_rounded, $duration) {
                return (Carbon::parse($v['time_end']) < Carbon::parse('10:50:00') && $end_time_rounded <= Carbon::parse('10:45:00'));
            });
        }

        if (!$pair) {
            $period = \Carbon\CarbonPeriod::create($start_time_rounded, $end_time_rounded);
            $pair = array_filter($this->pairs, function ($v) use ($start_time_rounded, $end_time_rounded, $period) {
                return $period->overlaps(Carbon::parse($v['time_start']), Carbon::parse($v['time_end']));
            });
        }


        if (count($pair) == 1) {
            $pair = array_shift($pair)['id'];
        } else {
            $pair = array_reduce($pair, function ($sum, $val) {
                $sum[] = $val['id'];
                return $sum;
            });
            $pair = implode(',', $pair);
        }


        return $pair ? $pair : "";

    }

    function get_lecture($raw)
    {

        $email = $raw['member_id'];

        if ($email) {
            $sql = "select
                id,
                fullName,
                email
                from contingent_etalon_lecturer
                where  email like CONCAT('%','$email','%') ";

            $res = $this->db->query($sql);
            $this->db->checkError();
            if ($res) {
                return array_pop($res)['id'];

            }
            return null;
        }
        return null;


    }

    function get_group_name($raw)
    {


        $email = $raw['member_id'];

        if ($email) {

            $email = strtolower($email);
            $sql = "select
                c.id,
                c.login
                from contingent_etalon c
                where '$email' like CONCAT( LOWER(c.login),'%')
            ";

            $res = $this->db->query($sql);
            $this->db->checkError();

            if ($res) {

                return array_pop($res)['id'];
            }
            return null;
        }


        return null;
    }

    public function rollback($all = false)
    {
        $rollback = "update meet_logs_cls set `pair`=null,`group`=null,lecturer=null,processed=0,process_state=0 where 1";
        $rollback .= (!$all && $this->fromDate) ? (" and `date` >=" . "'" . $this->fromDate . "'") : null;
        $rollback .= (!$all && $this->toDate) ? (" and `date` <=" . "'" . $this->toDate . "'") : null;
        $res = $this->db->exec($rollback);
        $this->db->checkError();
        $this->output('Rolled back ' . $res . " raw(s)" );

        return $res;
    }

    function saveRaw($item)
    {
        if(!$item['pair'] && !$item['group'] && !$item['lecturer'] ) { return false;}
        $item['processed'] = 1;
        if ($item['pair'] && ($item['group'] || $item['lecturer'])) {
            $item['process_state'] = 1;
        }
        $UpdateFields = ['pair', 'group', 'processed', 'process_state', 'lecturer'];
        $statement = array_reduce($UpdateFields, function ($accum, $current) use ($item) {
            if (!$item[$current]) {
                return $accum;
            }
            $tt = is_numeric($item[$current]) ? $item[$current] : ("'" . $item[$current] . "'");
            $accum[] = "`" . $current . "`" . "=" . $tt;
            return $accum;
        });
        $statement = implode(',', $statement);

        $id = $item['id'];
        $sql = "update `meet_logs_cls` set $statement WHERE id = $id";
        $this->db->exec($sql);
        if ($this->db->isError()) {
            echo $this->db->getLastError();
            throw new Exception($this->db->getLastError());

        }
        return true;
    }


}
