<?php


if (file_exists('../DevConfig.php')) {
    $config = include '../DevConfig.php';
} else {
    $config = include '../config.php';
}
require '../vendor/autoload.php';
use Carbon\Carbon;
include '../Database/database.php';
include '../Database/data_access.php';

class BBB_loader {


    private $db=null;
    public function __construct($db)
    {
        $this->db = $db;
    }


    function findMeet($meetingID)
    {

        $sql = "select * from meets where meetingID = ?";
        $meet = $this->db->query($sql,['types'=>'s','values' =>[$meetingID]]);
        $this->db->checkError();

        if($meet)
        {
            return $meet[0];
        }
        return false;
    }
    public  function createMeet($data)
    {


        $sql = "insert into meets(*fields) values (*values)";
        $date =  $data['createDate'];
        $data['createDate'] = Carbon::parse($date)->format('Y-m-d');
        $data['createTime'] = Carbon::parse($date)->format('h:i:s');
        $parse = $this->parse($sql,$data);
        $res = $this->db->exec($parse['sql'],['types'=>$parse['types'],'values' => $parse['bind']]);

        if($res)
        {
           return  $this->findMeet($data['meetingID']);
        }
        return  false;
    }
    public function FindOrCreateMeet($data){

        $meet = $this->findMeet($data['meetingID']);
        return $meet ?  $meet : $this->createMeet($data);

    }

    public function AddAttendee($attendee,$meet){
        $sql = "select * from attendees where meet_id=? and userID=?";
        $result = $this->db->query($sql,['types'=>'is','values' => [
            $meet['id'],
            $attendee['userID']
        ]]);


        if(!$result)
        {

            $attendee['meet_id'] = $meet['id'];

            $add =  "insert into attendees(*fields) values(*values)";
            $parse = $this->parse($add,$attendee);
            return $this->db->exec($parse['sql'],['types'=>$parse['types'],'values' =>$parse['bind']]);

        }
        return false;
    }

    public function postPrepareMeet($data){
        //Пересчитываем кол-во участников, группы,пару и продолжитьльность
        return true;
    }
    function parse($sql,$data,$requestType='insert'){
        $bind =array_values($data);
        $types = array_reduce($bind,function ($accum,$item){
            if(gettype($item) === 'integer')
            {
                $accum.='i';
            }
            if(gettype($item) === 'string')
            {
                $accum.='s';
            }
            return $accum;
        });
        switch ($requestType)
        {
            case 'insert':{
                $fields = implode(',',array_keys($data));
                $values = implode(',',array_fill(0,count($data),'?'));
                $sql = str_replace('*fields',$fields,$sql);
                $sql = str_replace('*values',$values,$sql);
                return [
                    'sql'=>$sql,
                    'bind' => $bind,
                    'types' => $types,
                ];
            }
        }



    }
}
function get_api_data($config){

    $url = $config["bbb"]['url'];
    $ch = curl_init();
    curl_setopt($ch,CURLOPT_URL,$url);
    curl_setopt($ch,CURLOPT_RETURNTRANSFER,true);
    curl_setopt($ch,CURLOPT_HTTPHEADER,['content-type'=>'text/xml']);
    $exec = curl_exec($ch);
    $httpCode = curl_getinfo($ch,CURLINFO_HTTP_CODE);
    curl_close($ch);
    if($httpCode === 200)
    {
        $result = new \SimpleXMLElement($exec);
        $result = json_decode(json_encode($result),true);
        if($result['returncode'] === "SUCCESS" && $result['messageKey'] !== "noMeetings")
        {
            return $result['meetings'];
        }
        return false;
    }

    return false;

}

$data = get_api_data($config);
if($data)
{


    $loader = new BBB_loader(new DataConnectionManager($config));
    foreach ($data as $item)
    {


        $meet = $loader->FindOrCreateMeet(array_slice($item,0,22));

        $changed = false;
        foreach ($item['attendees'] as $attendee)
        {
            $changed = $loader->AddAttendee($attendee,$meet);
            break;
        }
        $changed ? $loader->postPrepareMeet($meet) : '';

    }
    return true;
}
echo "Массив пуст";
return false;