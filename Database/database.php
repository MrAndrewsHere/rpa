<?php
/*
 * Класс управления соединением с БД.
 * Позволяет выполнять запросы с подготовкой, запросы с управляемой транзакцией, специфические утилитарные запросы.
 *
 * Поддерживает загрузку в базу файлов csv комендой LOAD INFILE при условии предоставлений глобального права пользователю
 * на загрузку файла.
 *
 * @autor Максим Михеев mikheevma@uni-dubna.ru
 *
 * */


class DataConnectionManager
{
    //TODO: покрыть класс тестами.
    private $connection=null;
    private $configuration;
    private $last_error;
    private $error_flag=false;

    /**
     * DataConnectionManager constructor.
     * @param array $config параметр подключения к базе в формате $c['database']['param']
     * @param bool $is_local_load флаг опции разрешения загрузки csv в таблицу
     */
    public function __construct($config,$is_local_load=false)
    {
        $this->configuration=$config;

        $this->connection=new mysqli();
        $this->connection->options(MYSQLI_OPT_LOCAL_INFILE, $is_local_load);
        $this->connection->real_connect(
            $config['database']['host'],
            $config['database']['user'],
            $config['database']['pass'],
            $config['database']['database']
        );

        if($this->connection->connect_error)
        {
            $this->error_flag=true;
            $this->last_error="DCM - db connect error: ".$this->connection->connect_errno.': '.$this->connection->connect_error;
        }

    }

    /**
     * закрывает соединение при смерти класса
     */
    public function __destruct()
    {
        if (!is_null($this->connection))
        {
            $this->connection->close();
        }
    }

    /**
     * Базовые запросы SELECT
     * @param string $sql
     * @param array $bind
     * @return array|bool вернет ассоциативный массив вида $a[[0]=>['field'=>'val']] или false в случае неудачи
     */
    public function query(string $sql,array $bind=[])
    {
        $this->error_flag=false;
        $data_set = array();
        if(($stmt=$this->connection->prepare($sql))==false)
        {
            $this->error_flag=true;
            $this->last_error="DCM - incorrect sql prepare: ".$sql." error: ".$this->connection->error;
            return false;
        }
        if(count($bind)!=0) {
            $stmt->bind_param($bind['types'], ...$bind['values']);
        }
        if($stmt->execute())
        {
            $result = $stmt->get_result();
            while($row = $result->fetch_assoc())
            {
                $data_set[]=$row;
            }
        }
        else
        {
            $this->error_flag=true;
            $this->last_error="DCM - sql error: ".$stmt->error;
        }
        $stmt->close();

        return $data_set;
    }

    /**
     * выполнение запросов управления INSERT, DELETE, UPDATE
     * @param string $sql запрос в виде строки с подготавливаемыми параметрами в виде ?
     * @param array $bind массив вида $b['types'=>'sss','values'=>[val,val,val]] содержит описание типов переменных и их значения
     * @param string $checker_func функция обратного вызова, которая позволяет проверять количество затронутых строк
     * @return bool|int количество затронутых строк или false в случае ошибки подготовки запроса
     */
    public function exec(string $sql,array $bind=[],$checker_func='')
    {
        $this->error_flag=false;
        if(($stmt=$this->connection->prepare($sql))===false)
        {
            $this->error_flag=true;
            $this->last_error="DCM - incorrect sql prepare: ".$sql." error: ".$this->connection->error;
            return false;
        }

        if(count($bind)!=0) {
            $stmt->bind_param($bind['types'], ...$bind['values']);
        }
        $this->connection->autocommit(false);
        if(!$stmt->execute())
        {
            $this->error_flag=true;
            $this->last_error="DCM - sql error: ".$stmt->error;
            $this->connection->rollback();
        }

        $rows_added=$this->connection->affected_rows;
        if(!empty($checker_func) && !$checker_func($rows_added))
        {
            $this->error_flag=true;
            $this->last_error="DCM - checking by user conditional fail, rows added: ".$rows_added;
            $this->connection->rollback();
        }

        if(!$this->error_flag)
        {
            $this->connection->commit();
        }
        $stmt->close();
        $this->connection->autocommit(true);
        return $rows_added;
    }

    public function query_v2($sql,$bind){
        return $this->query($sql,$this->preparation($bind));
    }

    function preparation($data){
        $values = array_values($data);
        $types = array_reduce($values,function ($accum,$item){
            if(gettype($item) === 'integer')
            {
                $accum.='i';
            }
            if(gettype($item) === 'string' || gettype($item) === 'NULL')
            {
                $accum.='s';
            }
            return $accum;
        });
       return  ['types' =>$types,'values'=>$values];
    }


    /**
     * @param  string $sql запрос в виде строки c пометкой куда вставлять названия полей и ?
     * (пример. $sql = "insert into meets(*fields) values (*values)")
     * @param array $data ключи масива - поля в таблице
     * @return bool|int количество затронутых строк или false в случае ошибки подготовки запроса
     * TODO переделать на что-то нормальное
     */
    public function insert($sql,$data){
        $fields = implode(',',array_keys($data));
        $values = implode(',',array_fill(0,count($data),'?'));
        $sql = str_replace('*fields',$fields,$sql);
        $sql = str_replace('*values',$values,$sql);
        return $this->exec($sql,$this->preparation($data));

    }

    /**
     * Утилитарные вызовы типа LOAD INFILE, выполняет запрос без подготовки
     * @param string $sql запрос с параметрами
     * @return bool false в случае ошибки
     */
    public function exec_untl(string $sql)
    {
        $this->error_flag=false;
        if(!$this->connection->query($sql))
        {
            $this->error_flag=true;
            $this->last_error="DCM - exec util error: ".$this->connection->errno.' '.$this->connection->error;
            return false;
        }
        return true;
    }

    /**
     * Возвращает содержимое последней ошибки
     * @return string
     */
    public function getLastError()
    {
        return $this->last_error;
    }

    /**
     * Флаг определяющий была ли ошибка в ходе выполнения последней операции
     * @return bool
     */
    public function isError()
    {
        return $this->error_flag;
    }
    /**
     *
     * @return bool
     */
    public function checkError()
    {
        if($this->isError())
        {
            echo $this->getLastError();
            return true;
        }
        return false;
    }
}



