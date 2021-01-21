<?php


class ContingentEtalon
{
    private $db = null;
    private $limit = 1500;
   private $entry = 2;
    private $data = null;
    public function __construct($db)
    {
        $this->db = $db;
    }

    public function process(){

        $iterator=0;
        echo "\r" . PHP_EOL;
        while(($this->data = $this->get_data()) && $this->entry>0)
        {

            //$this->entry --;
           foreach ($this->data as $item)
           {
                $this->update_login($item);
               $iterator++;
               echo chr(27) . "[1A";
               echo "Processed $iterator ". PHP_EOL;
           }
            if(count($this->data) < $this->limit)
            {break;}

        }

    }
    function get_data(){

        $sql = "select
                c.id,
                c.groupName as etalonGroup,
                c.department,
                c.secondName,
                c.firstName,
                c.thirdName,
                c.login,
                f.fullName,
                f.groupName as freeGroup,
                f.freeLogin from contingent_etalon c
                inner join contingent_etalon_free f
                on CONCAT(LOWER(c.secondName),' ',LOWER(c.firstName),' ',LOWER(c.thirdName)) = LOWER(f.fullName)
                where c.login is null
               limit $this->limit";
        $array = $this->db->query($sql);
        $this->db->checkError();
        return $array;
    }
    function update_login($item){
        $sql = "update contingent_etalon set login = ? where id = ?";
        $res = $this->db->exec($sql, ['types'=>'si','values'=>[$item['freeLogin'],$item['id']]]);
        $this->db->checkError();
        return $res;
    }
    function rollback(){
        $sql = 'update contingent_etalon set login = null where 1';
        $res = $this->db->exec($sql);
        $this->db->checkError();
        echo $res?'Rollback done':'Error'.PHP_EOL;

    }



}