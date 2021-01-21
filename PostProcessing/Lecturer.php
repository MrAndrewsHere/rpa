<?php


class Lecturer
{
    private $db=null;
    private $data = null;
    private $mailsInfo = null;

    public function __construct($db)
    {
        $this->db = $db;

    }

    public function process(){

        if(($this->data = $this->get_lecturers()) && ($this->mailsInfo = $this->get_mails_info()))
        {


            echo "\r" . PHP_EOL;
            $iterator = 0;
            foreach ($this->mailsInfo as $mail){
               $result =  $this->try_attach_mail($mail);
               $result?  $iterator+=$result:null;
                echo chr(27) . "[1A";
                echo "Attached $iterator ". PHP_EOL;
            }
            return  $iterator;
        }
        return "Not found";
    }

    public function get_lecturers(){
        $sql = "select * from contingent_etalon_lecturer";
         $res = $this->db->query($sql);
        $this->db->checkError();
        return $res?$res:false;

    }
    public function get_mails_info(){
        $sql = "select from_email,from_name from mails  where  CHAR_LENGTH(from_name) > 10 group by from_email,from_name ";
        $res =  $this->db->query($sql);
        $this->db->checkError();
        return $res?$res:false;

    }


    public function try_attach_mail($mail){
//        $sql = "select
//                c.id,
//                c.fullName,
//                m.from_email ,
//                m.from_name
//                from contingent_etalon_lecturer c
//                inner join (select from_email,from_name from mails group by from_email,from_name) m
//                 on  c.fullName  like CONCAT('%',m.from_name,'%')
//
//                 where  CHAR_LENGTH(m.from_name) > 10 group by  m.from_email, m.from_name,  c.fullName,c.id";

        foreach ($this->data as $item)
        {
            $name = $mail['from_name'];
            $lName = $item['fullName'];

            if(strpos($lName,$name) !== false)
            {
               return $this->attach_mail($mail['from_email'],$item['id']);
            }
            $parseName = explode(' ',$name);
            $ggg =  explode(' ',$lName);
            $firstSecond =[$ggg[0],$ggg[1]];

           if(count(array_intersect($firstSecond,$parseName)) > 1)
           {
               return $this->attach_mail($mail['from_email'],$item['id']);
           }



        }
        return false;
    }
    function attach_mail($from_email,$lec_id){

        $get_lecture = "select * from contingent_etalon_lecturer where id = $lec_id ";
        $lecture = $this->db->query($get_lecture)[0];

        if($lecture['email'])
        {
            if(!in_array($from_email,explode(';',$lecture['email'])))
            {
                $from_email = $lecture['email'].";".$from_email;
            }
            else{
                $from_email = $lecture['email'];
            }


        }
        $update = "update contingent_etalon_lecturer set email = ? where id=?";

        $this->db->exec($update, ['types'=>'si','values'=>[$from_email,$lec_id]]);
        $this->db->checkError();
        return 1;
    }
    public function rollback(){
        $sql = "update contingent_etalon_lecturer set email=null where 1";
        $res =  $this->db->exec($sql);
        $this->db->checkError();
        echo $res ? 'Rolled back ' : 'No one';
        echo PHP_EOL;
        return $res;
    }

}