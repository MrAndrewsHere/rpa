<?php
/**
 * Created by PhpStorm.
 * User: mikheev.m.a
 * Date: 08.04.2020
 * Time: 11:47
 */

$config=include 'config.php';

include 'Database/database.php';
include 'Database/data_access.php';

$files = array();
if($files=scandir($config['path']['files'])) {
    array_shift($files);
    array_shift($files); //remove . and ..
    $count_files = count($files);
    error_log(PHP_EOL . 'GL files to load found: ' . $count_files, 3, $config['path']['log']);
    if ($count_files == 0) {
        exit();
    }

    //создать таблицу сырых данных
    $dm = new DataConnectionManager($config);
    $clear_table=new GroupClearTable($dm);

    foreach ($files as $file) {
        if (!is_file($config['path']['files'] . $file)) {
            continue;
        }
        if (strpos($file, 'loaded_') !== false || strpos($file, 'group') === false) {
            continue;
        }
        $ext = pathinfo($file, PATHINFO_EXTENSION);
        if ($ext == 'bulk' || $ext != 'csv') {
            continue;
        }

        $rows = file($config['path']['files'] . $file);
        $count_rows = count($rows);
        if ($count_rows == 0) {
            break;
        }
        error_log(PHP_EOL . 'GL file: ' . $file . ' row count: ' . $count_rows, 3, $config['path']['log']);
        $cls_data = array();
        foreach ($rows as $row) {
            $matches = array();
            $cls_item = array();

            $re_emails = '/\s([\w\d\.]+)@uni\-dubna\.ru/i';
            $re_date_time = '/"(\d{1,2})\s([\D\.]+)(\d{4}).+(\d{2}:\d{2}:\d{2})/i';

            //выделяем дату
            if (preg_match($re_date_time, $row, $matches)) {
                $month = trim(str_replace(['мар.', 'апр.', 'май'], ['03', '04', '05'], $matches[2]));
                $day = strlen($matches[1]) == 1 ? '0' . $matches[1] : $matches[1];
                $cls_item[] = join('-', [$matches[3], $month, $day]); //date
                $cls_item[] = $matches[4]; //times
            }
            $matches = array();
            //выделяем пользователя и группу
            if (preg_match_all($re_emails, $row, $matches)) {
                if (isset($matches[1])) {
                    if(isset($matches[1][2])) {
                        $cls_item[] = $matches[1][2]; //groupname
                        $cls_item[] = $matches[1][1]; //username
                    }
                    else
                    {
                        error_log(PHP_EOL . 'GL incorrect group data: ' . var_export($matches[1],true), 3, $config['path']['log']);
                        continue;
                    }
                }
            }
            else
            {
                error_log(PHP_EOL . 'GL not match data: ' . $row, 3, $config['path']['log']);
                continue;
            }
            $cls_data[] = $cls_item;
        }

        if (!empty($cls_data))
        {
            if(!$clear_table->loadCleanData($cls_data))
            {
                error_log(PHP_EOL . 'GL error load data: ' . $dm->getLastError(), 3, $config['path']['log']);
            }

            $count_loaded=$clear_table->countLoadLines();
            error_log(PHP_EOL . 'GL rows loaded: ' . $count_loaded, 3, $config['path']['log']);

            rename($config['path']['files'].$file,$config['path']['files']."loaded_".$file);
        }
    }

    error_log(PHP_EOL.'GL loading data end',3,$config['path']['log']);
}