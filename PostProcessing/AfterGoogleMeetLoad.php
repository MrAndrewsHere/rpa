<?php

if (file_exists('../DevConfig.php')) {
    $config = include '../DevConfig.php';
} else {
    $config = include '../config.php';

}

require '../vendor/autoload.php';

use Carbon\Carbon;

include '../Database/database.php';
require 'Attendance.php';
require 'LineChart.php';
require 'BarChart.php';


/**
 * даты включительно;
 *
 * Attendance
 * limit - sql limit; 0 или null - без лимита;
 * если кол-во итераций не задано посчитает все не обработанные записи;
 *
 * По умолчанию данные считаются от последней загруженной даты в таблице минус один день.
 * Можно задать свою дату через ключ --from=дата (пример: php AfterGoogleMeetLoad.php --from=2020-10-23)
 *
 *  php AfterGoogleMeetLoad.php --output=false  отключает вывод прогресса в консоль
 */
try {
    $options = getopt('', ["from::", "output::",]);

    $output = isset($options['output']) && $options['output'] === 'false' ? false : true;

    isset($options['from']) ?
        $fromDate = Carbon::parse($options['from'], new \DateTimeZone('Europe/Moscow'))->format('Y-m-d') :
        $fromDate = null;


    $db = new DataConnectionManager($config);

    $attendance = new Attendance($db, 5000, null);
    $attendance->from($fromDate)->withOutput($output)->process();

    $lineChart = new LineChartData($db);
    $lineChart->from($fromDate)->withOutput($output)->process();

    $barChart = new BarChart($db);
    $barChart->from($fromDate)->withOutput($output)->process();

    exit();
} catch (Exception $exception) {
    exit($exception->getMessage());
}

