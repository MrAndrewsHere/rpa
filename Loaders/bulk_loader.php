<?php
/**
 *
 * загружаем данные из Google Meet, полученные в формате csv
 * поля, которые должны быть в файле:
 *
 * Дата,
 * Название события,
 * Описание события,
 * Код встречи,
 * Идентификатор участника,
 * Внешние участники,
 * Тип клиента,
 * Адрес электронной почты организатора,
 * Тип продукта,
 * Продолжительность,
 * Имя участника,
 * IP-адрес,
 * Город,
 * Страна,
 * Длительность приема аудиосигнала,
 * Идентификатор мероприятия календаря,
 * Идентификатор видеовстречи,
 * Длительность отправки видеосигнала
 *
 */

ini_set('mysqli.allow_local_infile',1);

//TODO: сделать загрузку данных напрямую из Google по API.

$config=include 'config.php';

include 'Database/database.php';
include 'Database/data_access.php';

//загрузить файл
$files = array();
if($files=scandir($config['path']['files']))
{
    array_shift($files); array_shift($files); //remove . and ..
    $count_files=count($files);
    error_log(PHP_EOL.'files to load found: '.$count_files,3,$config['path']['log']);
    if($count_files==0)
    {
        exit();
    }

    //создать таблицу сырых данных
    $dm=new DataConnectionManager($config,true);
    $bulk_man=new MeetBulkTable($dm);
    $bulk_table=$bulk_man->createBulkTable();

    foreach ($files as $file)
    {
        if(!is_file($config['path']['files'].$file))
        {
            continue;
        }
        if(strpos($file,'loaded_')!==false)
        {
            continue;
        }
        $ext=pathinfo($file,PATHINFO_EXTENSION);
        if($ext=='bulk' || $ext!='csv')
        {
            continue;
        }

        $bulk_file=$config['path']['files'].$file.$config['path']['fext'];

        $rows=file($config['path']['files'].$file);
        //посчитать число строк файла
        $count_rows=count($rows);
        if($count_rows==0)
        {
            break;
        }
        error_log(PHP_EOL.'file: '.$file.' row count: '.$count_rows,3,$config['path']['log']);

        foreach ($rows as $num=>$row)
        {
            $matches=array();

            $cls_row=$row;
            //корректируем дату так, чтобы функция mariadb STR_TO_DATE не ломалась
            $re = '/^("[0-9\s]+)([\D\.]+)(\s[0-9]+\s)(г\.),/i';
            $cls_row=preg_replace_callback($re,function($matches){
                return $matches[1].str_replace(['мар.','апр.','мая','июн.','июл.','авг.','сен.','окт.','нояб.','дек.'],['Mar','Apr','May','Jun','Jul','Avg','Sep','Oct','Nov','Dec'],$matches[2]).$matches[3].',';
            },$row);
            file_put_contents($bulk_file,$cls_row,FILE_APPEND);
        }

        //загрузить данные в таблицу
        if(!$bulk_man->loadBulkData($bulk_file,$bulk_table))
        {
            error_log(PHP_EOL.'load data filed: '.$dm->getLastError(),3,$config['path']['log']);
            break;
        }
        //получить число строк в таблице сырых данных
        $count_bulked=$bulk_man->countLoadLines($bulk_table);
        if($count_bulked==$count_rows)
        {
            error_log(PHP_EOL.'load data complete, liens loaded: '.$count_bulked,3,$config['path']['log']);
        }
        else
        {
            error_log(PHP_EOL.'load data incorrect: '.$dm->getLastError(),3,$config['path']['log']);
            break;
        }
        //запустить загрузку в таблицу чистых данных
        $cls_table=new MeetClearTable($dm);
        if(!$cls_table->loadCleanData($bulk_table,$count_bulked))
        {
            error_log(PHP_EOL.'load clear data filed: '.$dm->getLastError(),3,$config['path']['log']);
            break;
        }

        //помечаем загруженные файлы
        unlink($bulk_file);
        rename($config['path']['files'].$file,$config['path']['files']."loaded_".$file);
    }

    error_log(PHP_EOL.'loading data end',3,$config['path']['log']);
}






