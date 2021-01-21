<?php

class MeetBulkTable
{
    private $dbman=null;

    public function __construct(DataConnectionManager $manager)
    {
        $this->dbman=$manager;
    }

    function createBulkTable()
    {
        $table_name = 'meet_logs_bulk_'.date('dmY');

        $sql="DROP TABLE IF EXISTS ".$table_name;
        $this->dbman->exec($sql);

        $sql = "CREATE TABLE `" . $table_name . "` (
          `datetimes` varchar(100) DEFAULT NULL,
          `event_name` varchar(300) DEFAULT NULL,
          `event_desc` varchar(300) DEFAULT NULL,
          `meet_code` varchar(200) DEFAULT NULL,
          `member_id` varchar(200) DEFAULT NULL,
          `external_member` varchar(200) DEFAULT NULL,
          `client_type` varchar(200) DEFAULT NULL,
          `org_email` varchar(200) DEFAULT NULL,
          `product_type` varchar(200) DEFAULT NULL,
          `duration` varchar(200) DEFAULT NULL,
          `member_name` varchar(200) DEFAULT NULL,
          `ipv4` varchar(200) DEFAULT NULL,
          `city` varchar(200) DEFAULT NULL,
          `country` varchar(200) DEFAULT NULL,
          `duration_sending_audio` varchar(200) DEFAULT NULL,
          `id_calendar` varchar(200) DEFAULT NULL,
          `id_meet` varchar(200) DEFAULT NULL,
          `duration_sending_video` varchar(200) DEFAULT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8";

        $this->dbman->exec($sql);

        return $table_name;
    }

    function loadBulkData($file, $table)
    {
        $sql="load data infile '".$file."' into table `".$table."` FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\r\n'";
        $this->dbman->exec_untl($sql);
        return !$this->dbman->isError();
    }

    function countLoadLines($table)
    {
        $sql="select count(*) as cnt from ".$table;
        $res=$this->dbman->query($sql);
        return isset($res[0]['cnt'])?$res[0]['cnt']:0;
    }
}

class MeetClearTable
{
    private $dbman=null;

    public function __construct(DataConnectionManager $manager)
    {
        $this->dbman=$manager;
    }

    public function loadCleanData($bulk_table_name,$control_count_lines)
    {
        //TODO: сделать учет дублей при загрузке данных за несколько дней подряд.
        $sql="insert into `meet_logs_cls` (`date`,`times`,`event_name`,`event_desc`,`meet_code`,`member_id`,`external_member`,`client_type`,`org_email`,`product_type`, 
        `duration`,`member_name`,`ipv4`,`city`,`country`,`duration_sending_audio`,`id_calendar`,`id_meet`,`duration_sending_video`) 
        select STR_TO_DATE(TRIM(SUBSTRING_INDEX(ml.`datetimes`,',',1)),'%d %b %Y'), STR_TO_DATE(REPLACE(SUBSTRING_INDEX(ml.`datetimes`,',',-1),' GMT+3',''),'%H:%i:%S'), 
        ml.`event_name`,ml.`event_desc`,ml.`meet_code`,ml.`member_id`,ml.`external_member`,ml.`client_type`,ml.`org_email`,ml.`product_type`, 
        IF(TRIM(ml.`duration`)='', 0, CONVERT(TRIM(REPLACE(ml.`duration`,'\r\n','')),INTEGER)), ml.`member_name`,ml.`ipv4`,ml.`city`,ml.`country`,
        IF(TRIM(ml.`duration_sending_audio`)='',0, CONVERT(TRIM(ml.`duration_sending_audio`),INTEGER)), ml.`id_calendar`,ml.`id_meet`, 
        IF(TRIM(REPLACE(REPLACE(ml.`duration_sending_video`,'\r',''),'\n',''))='',0,CONVERT(TRIM(REPLACE(REPLACE(ml.`duration_sending_video`,'\r',''),'\n','')),INTEGER)) from `".
            $bulk_table_name."` ml";

        //начать транзакцию
        $this->dbman->exec($sql,[],function($rows_added) use($control_count_lines) {
            //получить число строк чистых данных
            return $rows_added==$control_count_lines;
        });
        //закрыть транзакцию

        return !$this->dbman->isError();
    }

}

class GroupClearTable
{
    private $dbman=null;

    public function __construct(DataConnectionManager $manager)
    {
        $this->dbman=$manager;
    }

    public function loadCleanData(array $cls_data)
    {
        $sql="insert into `group_emails_cls` (`date`,`times`,`groupname`,`username`) values (?, ?, ?, ?);";
        foreach ($cls_data as $row)
        {
            $this->dbman->exec($sql,['types'=>'ssss','values'=>$row]);
            if($this->dbman->isError())
            {
                break;
            }
        }
        return !$this->dbman->isError();
    }

    function countLoadLines()
    {
        $sql="select count(*) as cnt from `group_emails_cls`";
        $res=$this->dbman->query($sql);
        return isset($res[0]['cnt'])?$res[0]['cnt']:0;
    }
}