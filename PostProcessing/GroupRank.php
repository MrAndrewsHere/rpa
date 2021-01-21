<?php

require "BasePostProcess.php";

class GroupRank extends BaseClass
{
    use BaseMethods;

    public function process($callback = null)
    {
        $process = function () {
            foreach ($this->data as $item) {
                $temp = $item;
                $this->getRank($temp);
               if($this->saveRaw($temp)){
                   $this->counter++;
                   $this->output($this->format(),true);
               }
            }
        };

        parent::process($process);

    }


    function getRank(&$item){

        $index = "groupRank";

        if (strpos(strtolower($item['login']), ".sdo") !== false) {
            $item[$index] = 'sdo';
           return true;

        }
        if (strpos(strtolower($item['login']), ".z") !== false || strtolower($item['groupName']) === "заочники" || strpos(strtolower($item['groupName']), "-С") !== false || strpos(strtolower($item['groupName']), "-С") !== false) {
            $item[$index] = 'zaochniki';
            return true;
        }

        if (strpos(strtolower($item['login']), ".asp") !== false || strpos(strtolower($item['groupName']), "A") !== false || strpos(strtolower($item['groupName']), "А") !== false) {
            $item[$index] = 'aspiranti';
            return true;

        }

        if (strpos(strtolower($item['login']), ".m") !== false) {
            $item[$index] = 'magistri';
            return true;

        }
        if (strpos(strtolower($item['login']), ".dino") !== false) {
            $item[$index] = 'dino';
            return true;
        }
        if (strpos(strtolower($item['login']), ".fil") !== false) {
            $item[$index] = 'fill';
            return true;
        }
        if (strpos(strtolower($item['groupName']), "-ОЗ") !== false) {
            $item[$index] = 'OZ';
            return true;
        }


        if (strpos(strtolower($item['groupName']), "-М") !== false) {
            $item[$index] = 'M';
            return true;
        }
        $item[$index] = "default";
        return true;

    }

    function saveRaw($raw){

        $rank = $raw['groupRank'];
        $sql = "update contingent_etalon set groupRank = ? where id = ?";
         $this->db->exec($sql, [
            'types' => 'si',
            'values'=>[$raw['groupRank'],$raw['id']]
        ]);
        return !$this->db->checkError();

    }
    function getData()
    {
        $sql = "select id,groupName,login from contingent_etalon where groupRank is null ";
        $this->limit && $this->limit > 0 ? $sql .= " limit $this->limit" : null;
        $res = $this->db->query($sql);
        if ($this->db->checkError()) {
            return false;
        }
        if(count($res) === 0 && $this->data)
        {
            $this->output('I didnt find anything');
            return false;
        }
        return $res;
    }
    function rollback(){
        $sql = "update contingent_etalon set groupRank = null where 1";
        $res = $this->db->exec($sql);
        $this->db->checkError();
        $this->output('Rolled back ' . $res . " raw(s)" );

        return $res;
    }


}